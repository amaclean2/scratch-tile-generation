import asyncio
import time
import logging
import os
import gc
import boto3

from generate_tiles import generate_tile
from utils import get_tile_ranges_for_zoom

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3', region_name=os.getenv('AWS_REGION', 'us-east-1'))

TARGET_ZOOM_LEVELS = [6, 8, 10]

async def generate_all_tiles_for_variable(dataset, timestamp, forecast_hour, variable, progress, context):
  tiles_generated = 0
  max_concurrent_uploads = int(os.getenv('MAX_CONCURRENT_UPLOADS', '10'))
  upload_semaphore = asyncio.Semaphore(max_concurrent_uploads)
  
  logger.info(f"Processing variable: {variable}")
  variable_start = time.time()
  
  for zoom in TARGET_ZOOM_LEVELS:
    if zoom in progress.get('completed_zooms', []):
      logger.info(f"Zoom {zoom} already completed for {variable}")
      continue
    
    tiles_generated += await process_zoom_level(
      dataset, timestamp, forecast_hour, variable, zoom, upload_semaphore, progress, context
    )
    
    progress['completed_zooms'].append(zoom)
    
    gc.collect()
    
    if context.get_remaining_time_in_millis() < 60000:
      logger.warning(f"Low time remaining, stopping at zoom {zoom}")
      break
  
  variable_time = time.time() - variable_start
  logger.info(f"Completed {variable} in {variable_time:.1f}s")
  
  return tiles_generated

async def process_zoom_level(dataset, timestamp, forecast_hour, variable, zoom, semaphore, progress, context):
  batch_start = time.time()
  tile_ranges = get_tile_ranges_for_zoom(zoom)
  
  batch_size = 25 if zoom >= 10 else 50 if zoom >= 8 else 100
  upload_tasks = []
  tiles_generated = 0
  
  total_tiles = (tile_ranges['x_max'] - tile_ranges['x_min'] + 1) * \
                (tile_ranges['y_max'] - tile_ranges['y_min'] + 1)
  
  logger.info(f"Generating {total_tiles} tiles for {variable} zoom {zoom}")
  
  start_x = progress.get('last_x', tile_ranges['x_min']) if progress.get('current_zoom') == zoom else tile_ranges['x_min']
  start_y = progress.get('last_y', tile_ranges['y_min']) if progress.get('current_zoom') == zoom else tile_ranges['y_min']
  
  for x in range(start_x, tile_ranges['x_max'] + 1):
    y_start = start_y if x == start_x else tile_ranges['y_min']
    
    for y in range(y_start, tile_ranges['y_max'] + 1):
      try:
        tile_data = generate_tile(x, y, zoom, variable, dataset)
        
        if tile_data:
          year, month, day, hour = timestamp.split('/')
          sortable_timestamp = f"{year}{month}{day}{hour}"
          s3_key = f"hrrr/{sortable_timestamp}/{forecast_hour}/{variable}/{zoom}/{zoom}_{x}_{y}.png"
          
          upload_tasks.append(upload_tile_to_s3(tile_data, s3_key, semaphore))
          tiles_generated += 1
          
          if len(upload_tasks) >= batch_size:
            await asyncio.gather(*upload_tasks, return_exceptions=True)
            upload_tasks.clear()
        
        progress['last_x'] = x
        progress['last_y'] = y
        progress['current_zoom'] = zoom
        
        if context.get_remaining_time_in_millis() < 30000:
          logger.warning(f"Low time remaining, stopping at tile {x},{y}")
          if upload_tasks:
            await asyncio.gather(*upload_tasks, return_exceptions=True)
          return tiles_generated
            
      except Exception as e:
        logger.warning(f"Failed to generate tile {zoom}/{x}/{y}: {e}")
  
  if upload_tasks:
    await asyncio.gather(*upload_tasks, return_exceptions=True)
  
  batch_time = time.time() - batch_start
  logger.info(f"Completed {variable} zoom {zoom}: {batch_time:.1f}s, {tiles_generated} tiles")
  
  return tiles_generated

async def upload_tile_to_s3(tile_data, s3_key, semaphore):
  async with semaphore:
    try:
      loop = asyncio.get_event_loop()
      await loop.run_in_executor(
        None,
        lambda: s3_client.put_object(
            Bucket=os.getenv('S3_TILES_BUCKET', 'custom-tiles'),
            Key=s3_key,
            Body=tile_data,
            ContentType='image/png',
            CacheControl='max-age=3600, public, immutable'
        )
      )
    except Exception as e:
      logger.error(f"Failed to upload tile {s3_key}: {e}")
      raise