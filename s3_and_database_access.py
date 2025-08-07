from utils import build_most_recent_file_stamp, build_s3_filename
from pymongo import MongoClient

import logging
import boto3
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

async def look_for_current_tiles():
  """Check if current tiles exist in MongoDB"""
  try:
    current_timestamp = build_most_recent_file_stamp()
    
    tile_status = db.tile_status.find_one({
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

async def check_for_current_weather_files():
  """Check if current weather files exist in S3"""
  current_timestamp = build_most_recent_file_stamp()
  
  try:
    # Check if at least one file exists (hour 01)
    test_file = build_s3_filename(current_timestamp, '01')
    
    s3_client.head_object(
        Bucket=os.getenv('S3_WEATHER_BUCKET', 'paladinoutputs'),
        Key=test_file
    )
    
    logger.info(f"Weather files found for timestamp: {current_timestamp}")
    return True
      
  except s3_client.exceptions.NoSuchKey:
    logger.info(f"No weather files found for timestamp: {current_timestamp}")
    return False
  except Exception as error:
    logger.error(f"Error checking for weather files: {error}")
    raise

async def download_netcdf_file(s3_key, local_path):
  """Download NetCDF file from S3"""
  try:
    # Ensure directory exists
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    if os.path.exists(local_path):
      logger.info(f"File already exists: {local_path}")
      return True
    
    logger.info(f"Downloading: {s3_key} -> {local_path}")
    
    s3_client.download_file(
      os.getenv('S3_WEATHER_BUCKET', 'paladinoutputs'),
      s3_key,
      local_path
    )
    
    logger.info(f"Downloaded: {s3_key}")
    return True
      
  except Exception as error:
    logger.error(f"Failed to download {s3_key}: {error}")
    raise