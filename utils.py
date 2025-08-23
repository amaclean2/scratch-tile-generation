from datetime import datetime, timezone
import math
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def build_most_recent_file_stamp(override=None):

  if override:
    logger.info(f"Using override timestamp: {override}")
    return override
  
  current_time = datetime.now(timezone.utc)
  year = current_time.year
  month = str(current_time.month).zfill(2)
  day = str(current_time.day).zfill(2)
  hour = '12' if current_time.hour >= 12 else '00'
  
  return f"{year}/{month}/{day}/{hour}"


def create_local_netcdf_path(filename):
	os.makedirs("/tmp", exist_ok=True)
	return f"/tmp/{filename}"


def build_s3_filename(current_timestamp, forecast_hour='03'):
	splits = current_timestamp.split('/')
	year, month, day, hour = splits
	
	return f"hrrr/{current_timestamp}/log_{year}_{month}_{day}_{hour}_{forecast_hour}.nc"
 

def get_resolution_for_zoom(zoom):
	n = math.pow(2, zoom)
	tile_degrees_width = 360 / n
	return get_resolution(tile_degrees_width, zoom)


def get_resolution(bounds_diff, zoom=None):
	if bounds_diff > 50:
		base_resolution = 32
	elif bounds_diff > 10:
		base_resolution = 16
	elif bounds_diff > 2:
		base_resolution = 8
	elif bounds_diff > 0.5:
		base_resolution = 4
	else:
		base_resolution = 2
	
	if zoom is not None:
		if zoom < 6:
			return max(2, math.floor(base_resolution / 4))
		elif zoom < 8:
			return max(2, math.floor(base_resolution / 2))
	
	return 2


def binary_search(arr, target):
	low = 0
	high = len(arr) - 1
	while low <= high:
		mid = math.floor((low + high) / 2)
		if arr[mid] > target:
			high = mid - 1
		else:
			low = mid + 1
	return low

def alpha_color(r, g, b):
	valid_colors = [c for c in [r, g, b] if c > 0]
	if len(valid_colors) > 0:
		return min(255, (sum(valid_colors) / len(valid_colors)) * 2)
	return 0


def fahrenheit_to_celsius(fahrenheit):
	return ((fahrenheit - 32) * 5) / 9


def celsius_to_fahrenheit(celsius):
    return (celsius * 9) / 5 + 32
  

def get_lambda_tmp_space():
	try:
		statvfs = os.statvfs('/tmp')
		available_bytes = statvfs.f_frsize * statvfs.f_bavail
		return available_bytes / (1024 * 1024)  # Return MB
	except:
		return 512  # Default assumption


def cleanup_tmp_directory():
	tmp_dir = "/tmp"
	if os.path.exists(tmp_dir):
		for file in os.listdir(tmp_dir):
			file_path = os.path.join(tmp_dir, file)
			try:
				if os.path.isfile(file_path):
					os.remove(file_path)
			except Exception:
				pass
  

def get_tile_ranges_for_zoom(zoom):
  match zoom:
    case 6:
      return {'x_min': 9, 'x_max': 20, 'y_min': 21, 'y_max': 28}
    case 8:
      return {'x_min': 39, 'x_max': 81, 'y_min': 86, 'y_max': 113}
    case 10:
      return {'x_min': 156, 'x_max': 324, 'y_min': 347, 'y_max': 453}
    case _:
      return {'x_min': 0, 'x_max': 3, 'y_min': 0, 'y_max': 3}