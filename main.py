import asyncio
from read_net_cdf import read_weather
from s3_and_database_access import check_for_current_weather_files, download_netcdf_file, look_for_current_tiles
import logging

from utils import build_most_recent_file_stamp, build_s3_filename, create_local_netcdf_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
FORECAST_HOURS = 48
TILE_SIZE = 256
    
async def convert_weather_to_tiles():
  """Convert weather data to tiles"""
  current_timestamp = build_most_recent_file_stamp()
  
  # Test with just the first forecast hour
  i = 0
  forecast_hour = str(i + 1).zfill(2)
  
  s3_netcdf_file = build_s3_filename(current_timestamp, forecast_hour)
  filename = s3_netcdf_file.split('/')[-1]
  local_netcdf_path = create_local_netcdf_path(filename)
  
  # Download the file
  await download_netcdf_file(s3_netcdf_file, local_netcdf_path)
  
  # Read the weather data
  await read_weather(local_netcdf_path)
  
  logger.info("Successfully read weather data")
  return True



async def cleanup():
  """Cleanup function"""
  # TODO: Clean up temporary files, old tilesets, etc.
  return True

async def main():
  """Main execution function"""
  try:
    current_tiles_exist = await look_for_current_tiles()
    if current_tiles_exist:
        logger.info("Current tiles already exist, exiting")
        return
        
    files_exist = await check_for_current_weather_files()
    if not files_exist:
        logger.info("No weather files available, exiting")
        return
        
    await convert_weather_to_tiles()
      
  except Exception as error:
    logger.error(f"Error making tiles: {error}")
    raise
  finally:
    await cleanup()

if __name__ == "__main__":
  asyncio.run(main())