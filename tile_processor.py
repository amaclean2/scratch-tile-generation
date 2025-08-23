import asyncio
import time
import logging
import os
import gc
from datetime import datetime, timezone

from read_net_cdf import read_weather
from s3_and_database_access import download_multiple_netcdf_files, db
from tile_generator import generate_all_tiles_for_variable
from utils import build_most_recent_file_stamp, build_s3_filename, create_local_netcdf_path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TARGET_ZOOM_LEVELS = [6, 8, 10]

async def process_single_variable(variable, forecast_hour, context, override=None):
  try:
    current_timestamp = build_most_recent_file_stamp(override=override)

    s3_netcdf_file = build_s3_filename(current_timestamp, forecast_hour)
    filename = s3_netcdf_file.split('/')[-1]
    local_netcdf_path = create_local_netcdf_path(filename)
    
    downloaded_files = await download_multiple_netcdf_files([(s3_netcdf_file, local_netcdf_path, forecast_hour)])
    
    if not downloaded_files:
      logger.error("Failed to download NetCDF file")
      return {'status': 'error', 'reason': 'download_failed'}
    
    local_path, _ = downloaded_files[0]
    weather_data = await read_weather(local_path)
    
    if weather_data is None:
      logger.warning(f"No weather data for variable {variable}")
      return {'status': 'error', 'reason': 'no_data'}
    
    progress = get_variable_progress(current_timestamp, variable)
    
    tiles_generated = await generate_all_tiles_for_variable(
      weather_data, current_timestamp, forecast_hour, variable, progress, context
    )
    
    mark_variable_complete(current_timestamp, variable)
    
    weather_data.close()
    try:
      os.remove(local_path)
    except Exception as e:
      logger.warning(f"Could not remove {local_path}: {e}")
    
    logger.info(f"Successfully generated {tiles_generated} tiles for {variable}")
    return {'status': 'success', 'tiles_generated': tiles_generated}
    
  except Exception as error:
    logger.error(f"Error processing variable {variable}: {error}")
    raise

def get_variable_progress(timestamp, variable):
  """Get progress for a specific variable"""
  progress = db.tile_progress.find_one({
    'timestamp': timestamp,
    'variable': variable
  })
  
  if not progress:
    progress = {
      'timestamp': timestamp,
      'variable': variable,
      'completed_zooms': [],
      'current_zoom': TARGET_ZOOM_LEVELS[0],
      'last_x': 0,
      'last_y': 0,
      'status': 'in_progress'
    }
    db.tile_progress.insert_one(progress)
  
  return progress

def mark_variable_complete(timestamp, variable):
  """Mark variable as completely processed"""
  db.tile_progress.update_one(
    {'timestamp': timestamp, 'variable': variable},
    {'$set': {'status': 'complete', 'completed_at': datetime.now(timezone.utc)}}
  )