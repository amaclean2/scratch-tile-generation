import json
import asyncio
from read_net_cdf import read_weather
from s3_and_database_access import (check_for_current_weather_files,
  download_multiple_netcdf_files, look_for_current_tiles, mark_tiles_complete
)
from generate_tiles import WeatherDataProcessor, generate_tile
import logging
import os
import boto3
from utils import build_most_recent_file_stamp, build_s3_filename, create_local_netcdf_path, get_tile_ranges_for_zoom

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize S3 client for tile uploads
s3_client = boto3.client('s3', region_name=os.getenv('AWS_REGION', 'us-east-1'))

FORECAST_HOURS = ['01']  # Start with just one hour for Lambda
TILE_SIZE = 256
TARGET_ZOOM_LEVELS = [6, 8, 10]
VARIABLES = ['wspd', 'tmp', 'rh', 'mc1', 'mc10', 'mc100', 'mc1000', 'mcwood', 'mcherb', 'kbdi', 'ic', 'erc', 'bi', 'sc', 'gsi']

def lambda_handler(event, context):
  """Main Lambda handler function"""
  try:
    # Run the async function
    result = asyncio.run(process_weather_tiles())
    
    return {
      'statusCode': 200,
      'body': json.dumps({
        'message': 'Successfully processed weather tiles',
        'result': result
      })
    }
  except Exception as error:
    logger.error(f"Error in Lambda handler: {error}")
    return {
      'statusCode': 500,
      'body': json.dumps({
        'error': str(error)
      })
    }
    

async def process_weather_tiles():
  try:
    current_tiles_exist = await look_for_current_tiles()
    if current_tiles_exist:
      logger.info("Current tiles already exist, exiting")
      return {'status': 'skipped', 'reason': 'tiles_exist'}
        
    files_exist = await check_for_current_weather_files()
    if not files_exist:
      logger.info("No weather files available, exiting")
      return {'status': 'skipped', 'reason': 'no_files'}
    
    result = await convert_weather_to_tiles()
    return result
      
  except Exception as error:
    logger.error(f"Error processing weather tiles: {error}")
    raise
  finally:
    await cleanup()
    

async def convert_weather_to_tiles():
  current_timestamp = build_most_recent_file_stamp()
  
  download_tasks = []
  for forecast_hour in FORECAST_HOURS:
    
    s3_netcdf_file = build_s3_filename(current_timestamp, forecast_hour)
    filename = s3_netcdf_file.split('/')[-1]
    local_netcdf_path = create_local_netcdf_path(filename)
    
    download_tasks.append((s3_netcdf_file, local_netcdf_path, forecast_hour))
  
  downloaded_files = await download_multiple_netcdf_files(download_tasks)
  
  if not downloaded_files:
    logger.error("No files were downloaded successfully")
    return {'status': 'error', 'reason': 'download_failed'}

  total_tiles_generated = 0
  
  # test single file
  # ===========
  # local_path, forecast_hour = downloaded_files[0]
  # logger.info(f"Processing forecast hour {forecast_hour}")
  
  # weather_data = await read_weather(local_path)
  
  # if not weather_data:
  #   logger.warning(f"No weather data for forecast hour {forecast_hour}")
  #   return {'status': 'error', 'reason': 'no_weather_data'}
  
  # logger.info(f"Successfully read {len(weather_data)} data points for hour {forecast_hour}")
  
  # processor = WeatherDataProcessor(weather_data)
  # logger.info(f"Created processor with {len(processor.unique_lat_set)} lats, and {len(processor.unique_lng_set)} lngs")
  
  # test_zoom, test_x, test_y = 6, 15, 23
  # test_variable = 'tmp'

  # logger.info(f"Testing tile generation and S3 upload: {test_zoom}/{test_x}/{test_y} for {test_variable}")

  # tile_data = generate_tile(test_zoom, test_x, test_y, processor.data, test_variable)

  # if tile_data:
  #   logger.info(f"Generated tile: {len(tile_data)} bytes")
    
  #   # Test S3 upload
  #   year, month, day, hour = current_timestamp.split('/')
  #   sortable_timestamp = f"{year}{month}{day}{hour}"
  #   s3_key = f"hrrr/{sortable_timestamp}/{forecast_hour}/{test_variable}/{test_zoom}_{test_x}_{test_y}.png"
  #   logger.info(f"Uploading to S3: {s3_key}")
    
  #   try:
  #     await upload_tile_to_s3(tile_data, s3_key)
  #     logger.info(f"Successfully uploaded tile to S3")
      
  #     return {
  #       'status': 'success',
  #       'message': f'Generated and uploaded test tile',
  #       'data_points': len(weather_data),
  #       's3_key': s3_key,
  #       'timestamp': current_timestamp
  #     }
  #   except Exception as e:
  #     logger.error(f"S3 upload failed: {e}")
  #     return {'status': 'error', 'reason': 'upload_failed', 'error': str(e)}
      
  # else:
  #   logger.error("Failed to generate test tile")
  #   return {'status': 'error', 'reason': 'tile_generation_failed'}
  
  # ===========
  
  for local_path, forecast_hour in downloaded_files:
    logger.info(f"Processing forecast hour {forecast_hour}")
    
    weather_data = await read_weather(local_path)
    
    if not weather_data:
      logger.warning(f"No weather data for forecast hour {forecast_hour}")
      continue
        
    logger.info(f"Successfully read {len(weather_data)} data points for hour {forecast_hour}")
    
    processor = WeatherDataProcessor(weather_data)
    
    tiles_generated = await generate_all_tiles(processor, current_timestamp, forecast_hour)
    total_tiles_generated += tiles_generated
    
    try:
      os.remove(local_path)
      logger.info(f"Cleaned up {local_path}")
    except Exception as e:
      logger.warning(f"Could not remove {local_path}: {e}")
    
  await mark_tiles_complete(current_timestamp)
  
  logger.info(f"Successfully generated {total_tiles_generated} tiles")
  return {
    'status': 'success', 
    'tiles_generated': total_tiles_generated,
    'timestamp': current_timestamp
  }
  

async def generate_all_tiles(processor, timestamp, forecast_hour):
  tiles_generated = 0
  max_concurrent_uploads = int(os.getenv('MAX_CONCURRENT_UPLOADS', 10))
  upload_semaphore = asyncio.Semaphore(max_concurrent_uploads)
    
  for variable in VARIABLES:
    logger.info(f"Generating {variable} tiles for zoom {zoom}")
    
    upload_tasks = []
    
    for zoom in TARGET_ZOOM_LEVELS:
      tile_ranges = get_tile_ranges_for_zoom(zoom)
    
      for x in range(tile_ranges['x_min'], tile_ranges['x_max'] + 1):
        for y in range(tile_ranges['y_min'], tile_ranges['y_max'] + 1):
          try:
            tile_data = generate_tile(zoom, x, y, processor.data, variable)
            
            if tile_data:
              year, month, day, hour = timestamp.split('/')
              sortable_timestamp = f"{year}{month}{day}{hour}"
              s3_key = f"hrrr/{sortable_timestamp}/{forecast_hour}/{variable}/{zoom}_{x}_{y}.png"
              upload_tasks.append(upload_tile_to_s3(tile_data, s3_key, upload_semaphore))
              tiles_generated += 1
                    
          except Exception as e:
            logger.warning(f"Failed to generate tile {zoom}/{x}/{y} for {variable}: {e}")
            
      if upload_tasks:
        logger.info(f"Uploading {len(upload_tasks)} tiles for {variable} zoom {zoom}")
        await asyncio.gather(*upload_tasks, return_exceptions=True)
        logger.info(f"Completed uploads for {variable} zoom {zoom}")
        
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
  

async def cleanup():
  try:
    temp_dir = "/tmp"
    
    if os.path.exists(temp_dir):
      for file in os.listdir(temp_dir):
        file_path = os.path.join(temp_dir, file)
        try:
          os.remove(file_path)
          logger.info(f"Cleaned up {file_path}")
        except Exception as e:
          logger.warning(f"Could not remove {file_path}: {e}")

  except Exception as e:
    logger.warning(f"Cleanup warning: {e}")

  return True