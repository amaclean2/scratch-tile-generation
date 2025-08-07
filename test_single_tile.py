import asyncio
import logging
from read_net_cdf import read_weather
from generate_tiles import generate_tile
from s3_and_database_access import download_netcdf_file
from utils import build_most_recent_file_stamp, build_s3_filename, create_local_netcdf_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_single_tile():
    """Test generating a single tile to verify the pipeline works"""
    
    # Get current weather file
    current_timestamp = build_most_recent_file_stamp()
    forecast_hour = "01"  # First forecast hour
    
    s3_netcdf_file = build_s3_filename(current_timestamp, forecast_hour)
    filename = s3_netcdf_file.split('/')[-1]
    local_netcdf_path = create_local_netcdf_path(filename)
    
    try:
        # Download and read weather data
        logger.info("Downloading NetCDF file...")
        await download_netcdf_file(s3_netcdf_file, local_netcdf_path)
        
        logger.info("Reading weather data...")
        weather_data = await read_weather(local_netcdf_path)
        
        if not weather_data:
            logger.error("No weather data retrieved")
            return False
            
        logger.info(f"Got {len(weather_data)} weather data points")
        
        # Test tile coordinates - let's pick a tile somewhere in the continental US
        test_zoom = 6
        test_x = 15  # Roughly central US longitude
        test_y = 23  # Roughly central US latitude
        
        logger.info(f"Generating test tile: zoom {test_zoom}, x {test_x}, y {test_y}")
        
        # Test wind speed tile
        logger.info("Testing wind speed tile...")
        wind_tile = generate_tile(test_zoom, test_x, test_y, weather_data)
        
        if wind_tile:
            # Save to local file for inspection
            with open(f"test_wind_tile_{test_zoom}_{test_x}_{test_y}.png", "wb") as f:
                f.write(wind_tile)
            logger.info(f"Wind speed tile saved! Size: {len(wind_tile)} bytes")
        else:
            logger.warning("Wind speed tile generation returned None")
        
        # Test temperature tile
        logger.info("Testing temperature tile...")
        temp_tile = generate_tile(test_zoom, test_x, test_y, weather_data, variable='tmp')
        
        if temp_tile:
            # Save to local file for inspection
            with open(f"test_temp_tile_{test_zoom}_{test_x}_{test_y}.png", "wb") as f:
                f.write(temp_tile)
            logger.info(f"Temperature tile saved! Size: {len(temp_tile)} bytes")
        else:
            logger.warning("Temperature tile generation returned None")
            
        # Test humidity tile  
        logger.info("Testing humidity tile...")
        humidity_tile = generate_tile(test_zoom, test_x, test_y, weather_data, variable='rh')
        
        if humidity_tile:
            # Save to local file for inspection
            with open(f"test_humidity_tile_{test_zoom}_{test_x}_{test_y}.png", "wb") as f:
                f.write(humidity_tile)
            logger.info(f"Humidity tile saved! Size: {len(humidity_tile)} bytes")
        else:
            logger.warning("Humidity tile generation returned None")
        
        logger.info("Single tile test completed successfully!")
        return True
        
    except Exception as error:
        logger.error(f"Error in single tile test: {error}")
        raise

if __name__ == "__main__":
    asyncio.run(test_single_tile())