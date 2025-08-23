from datetime import datetime, timezone
from utils import build_most_recent_file_stamp, build_s3_filename
from pymongo import MongoClient
import asyncio

import logging
import boto3
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3_client = boto3.client(
  's3',
  region_name=os.getenv('AWS_REGION', 'us-east-1')
)

mongo_client = MongoClient(
  os.getenv('ATLAS_URI'),
  tls=True,
  tlsAllowInvalidCertificates=True
)
db = mongo_client.paladin

async def look_for_current_tiles(override=None):
  try:
    current_timestamp = build_most_recent_file_stamp(override=override)

    tile_status = db.tileStatus.find_one({
        'modelRun': current_timestamp
    })
    
    if not tile_status:
      logger.info(f"No tile status found for model run: {current_timestamp}")
      return False
        
    if tile_status.get('status') == 'complete':
      logger.info(f"Complete tiles found for model run: {current_timestamp}")
      return True
        
    return False
      
  except Exception as error:
    logger.error(f"Error checking for current tiles: {error}")
    raise

async def check_for_current_weather_files(override=None):
  current_timestamp = build_most_recent_file_stamp(override=override)

  try:
    test_file = build_s3_filename(current_timestamp, '03')
    
    logger.info(f"Checking for weather file: {test_file}")
    
    s3_client.head_object(
        Bucket=os.getenv('S3_WEATHER_BUCKET', 'paladinoutputs'),
        Key=test_file
    )
    
    logger.info(f"Weather files found for timestamp: {current_timestamp}")
    return True
      
  except Exception as e:
    error_code = e.response['Error']['Code']
    if error_code == '404':
      logger.info(f"No weather files found for timestamp: {current_timestamp}")
      return False
    else:
      logger.error(f"S3 error checking for weather files: {error_code} - {e}")
      raise
  except Exception as error:
    logger.error(f"Error checking for weather files: {error}")
    raise

async def download_multiple_netcdf_files(download_tasks):
  max_concurrent_downloads = int(os.getenv('MAX_CONCURRENT_DOWNLOADS', 2))
  semaphore = asyncio.Semaphore(max_concurrent_downloads)

  tasks = [download_netcdf_file(s3_key, local_path, forecast_hour, semaphore) 
    for s3_key, local_path, forecast_hour in download_tasks]
  
  # Wait for all downloads to complete
  results = await asyncio.gather(*tasks)
  
  # Return only successful downloads
  return [result for result in results if result is not None]

async def download_netcdf_file(s3_key, local_path, forecast_hour, semaphore):
  async with semaphore:
    # Ensure directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    if os.path.exists(local_path):
      logger.info(f"File already exists: {local_path}")
      success = True
    else:
      try:
        logger.info(f"Downloading: {s3_key} -> {local_path}")
        
        # Run the sync S3 download in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
          None,
          s3_client.download_file,
          os.getenv('S3_WEATHER_BUCKET', 'paladinoutputs'),
          s3_key,
          local_path
        )
        
        logger.info(f"Downloaded: {s3_key}")
        success = True
          
      except Exception as error:
        logger.error(f"Failed to download {s3_key}: {error}")
        success = False

  if forecast_hour is not None:
    return (local_path, forecast_hour) if success else None
  else:
    return success
  
  
async def mark_tiles_complete(timestamp):
  from s3_and_database_access import db
  
  try:
    db.tileStatus.update_one(
      {'modelRun': timestamp},
      {
        '$set': {
          'modelRun': timestamp,
          'status': 'complete',
          'completedAt': datetime.now(timezone.utc)
        }
      },
      upsert=True
    )
    logger.info(f"Marked tiles complete for {timestamp}")
  except Exception as e:
    logger.error(f"Failed to mark tiles complete: {e}")