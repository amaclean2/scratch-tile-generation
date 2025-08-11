import asyncio
import logging
import time
from read_net_cdf import read_weather
from generate_tiles import generate_tile, generate_tile_optimized, WeatherDataProcessor
from s3_and_database_access import download_netcdf_file
from utils import build_most_recent_file_stamp, build_s3_filename, create_local_netcdf_path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_performance_comparison():
	
	current_timestamp = build_most_recent_file_stamp()
	forecast_hour = "01"
	
	s3_netcdf_file = build_s3_filename(current_timestamp, forecast_hour)
	filename = s3_netcdf_file.split('/')[-1]
	local_netcdf_path = create_local_netcdf_path(filename)
	
	try:
		logger.info("Downloading and reading weather data...")
		await download_netcdf_file(s3_netcdf_file, local_netcdf_path)
		weather_data = await read_weather(local_netcdf_path)
		
		if not weather_data:
			logger.error("No weather data retrieved")
			return False
		
		logger.info(f"Got {len(weather_data)} weather data points")
		
		test_tiles = [
			(6, 15, 23),  # Central US
			(6, 16, 23),  # Adjacent tile
			(6, 15, 24),  # Adjacent tile
			(6, 16, 24),  # Adjacent tile
		]
		
		processor = WeatherDataProcessor(weather_data)
		
		for i, (zoom, x, y) in enumerate(test_tiles):
			tile = generate_tile_optimized(zoom, x, y, processor, variable='tmp')
			
			if tile:
				with open(f"optimized_tile_{i}_{zoom}_{x}_{y}.png", "wb") as f:
					f.write(tile)
			else:
				logger.warning(f"Optimized tile {i} failed")
		
		return True
			
	except Exception as error:
		logger.error(f"Error in performance test: {error}")
		raise

async def test_batch_generation():
		
	current_timestamp = build_most_recent_file_stamp()
	forecast_hour = "01"
	
	s3_netcdf_file = build_s3_filename(current_timestamp, forecast_hour)
	filename = s3_netcdf_file.split('/')[-1]
	local_netcdf_path = create_local_netcdf_path(filename)
	
	try:
		# Get weather data
		await download_netcdf_file(s3_netcdf_file, local_netcdf_path)
		weather_data = await read_weather(local_netcdf_path)
		
		if not weather_data:
				return False
		
		# Build processor once
		start_time = time.time()
		processor = WeatherDataProcessor(weather_data)
		setup_time = time.time() - start_time
		
		logger.info(f"Processor setup: {setup_time:.2f}s")
		
		# Generate a grid of tiles (zoom 6, covering parts of US)
		zoom = 6
		x_range = range(14, 18)  # 4 tiles wide
		y_range = range(22, 26)  # 4 tiles tall
		
		variables = ['tmp', 'rh', 'wspd']  # Test 3 variables
		
		total_tiles = len(x_range) * len(y_range) * len(variables)
		logger.info(f"Generating {total_tiles} tiles...")
		
		generation_start = time.time()
		tiles_generated = 0
		
		for variable in variables:
			for x in x_range:
				for y in y_range:
					tile = generate_tile_optimized(zoom, x, y, processor, variable)
					if tile:
						tiles_generated += 1
						# Save tile
						with open(f"batch_{variable}_{zoom}_{x}_{y}.png", "wb") as f:
							f.write(tile)
		
		generation_time = time.time() - generation_start
		total_time = time.time() - start_time
		
		logger.info(f"=== BATCH GENERATION RESULTS ===")
		logger.info(f"Setup time: {setup_time:.2f}s")
		logger.info(f"Generation time: {generation_time:.2f}s")
		logger.info(f"Total time: {total_time:.2f}s")
		logger.info(f"Tiles generated: {tiles_generated}/{total_tiles}")
		logger.info(f"Average per tile: {(generation_time/tiles_generated):.3f}s")
		
		return True
			
	except Exception as error:
		logger.error(f"Error in batch test: {error}")
		raise

if __name__ == "__main__":
		import sys
		
		if len(sys.argv) > 1 and sys.argv[1] == "batch":
				asyncio.run(test_batch_generation())
		else:
				asyncio.run(test_performance_comparison())