import asyncio
from read_net_cdf import read_weather
from s3_and_database_access import (check_for_current_weather_files,
  download_multiple_netcdf_files, look_for_current_tiles
)
import logging
import os

from utils import build_most_recent_file_stamp, build_s3_filename, create_local_netcdf_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
FORECAST_HOURS = 48
TILE_SIZE = 256
    
async def convert_weather_to_tiles():
  current_timestamp = build_most_recent_file_stamp()
  
  # Define which forecast hours you want to process
  # For now, let's do first few hours - you can expand this
  forecast_hours = ['01']  # Adjust as needed
  max_concurrent_downloads = int(os.getenv('MAX_CONCURRENT_DOWNLOADS', 2))
  
  logger.info(f"Processing {len(forecast_hours)} forecast hours with max {max_concurrent_downloads} concurrent downloads")
  
  # Download multiple files in parallel (with concurrency limit)
  download_tasks = []
  for forecast_hour in forecast_hours:
    s3_netcdf_file = build_s3_filename(current_timestamp, forecast_hour)
    filename = s3_netcdf_file.split('/')[-1]
    local_netcdf_path = create_local_netcdf_path(filename)
    
    download_tasks.append((s3_netcdf_file, local_netcdf_path, forecast_hour))
  
  # Download files with concurrency limit
  downloaded_files = await download_multiple_netcdf_files(download_tasks)

  # Process each downloaded file
  for local_path, forecast_hour in downloaded_files:
    logger.info(f"Processing forecast hour {forecast_hour}")
    
    # Read weather data
    weather_data = await read_weather(local_path)
    
    if weather_data:
      logger.info(f"Successfully read {len(weather_data)} data points for hour {forecast_hour}")
      
      # TODO: Generate tiles here using weather_data
      # processor = WeatherDataProcessor(weather_data)
      # for each tile: generate_tile_optimized(zoom, x, y, processor, variable)
        
    else:
      logger.warning(f"No weather data for forecast hour {forecast_hour}")
    
    # Clean up the NetCDF file after processing
    try:
      os.remove(local_path)
      logger.info(f"Cleaned up {local_path}")
    except Exception as e:
      logger.warning(f"Could not remove {local_path}: {e}")
  
  logger.info("Successfully processed all weather data")
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