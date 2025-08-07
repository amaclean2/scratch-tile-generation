from datetime import datetime, timezone
import math

def build_most_recent_file_stamp():
  current_time = datetime.now(timezone.utc)
  year = current_time.year
  month = str(current_time.month).zfill(2)
  day = str(current_time.day).zfill(2)
  hour = '12' if current_time.hour >= 12 else '00'
  
  return f"{year}/{month}/{day}/{hour}"

def create_tile_path():
  return "./tiles/something"

def create_local_netcdf_path(filename):
  return f"./temp/{filename}"

def build_s3_filename(current_timestamp, forecast_hour='01'):
  splits = current_timestamp.split('/')
  year, month, day, hour = splits
  
  return f"hrrr/{current_timestamp}/log_{year}_{month}_{day}_{hour}_{forecast_hour}.nc"

def convert_tile_to_coords(zoom, x, y):
    """Convert tile coordinates to geographic bounds"""
    n = math.pow(2, zoom)
    
    # Calculate longitude bounds
    west = (x / n) * 360 - 180
    east = ((x + 1) / n) * 360 - 180
    
    # Calculate latitude bounds (using inverse Mercator projection)
    north_lat_rad = math.atan(math.sinh(math.pi * (1 - (2 * y) / n)))
    south_lat_rad = math.atan(math.sinh(math.pi * (1 - (2 * (y + 1)) / n)))
    
    north = (north_lat_rad * 180) / math.pi
    south = (south_lat_rad * 180) / math.pi
    
    return {
        'left': west,
        'right': east,
        'top': north,
        'bottom': south
    }

def get_resolution_for_zoom(zoom):
    """Get data resolution for zoom level"""
    n = math.pow(2, zoom)
    tile_degrees_width = 360 / n
    return get_resolution(tile_degrees_width, zoom)

def get_resolution(bounds_diff, zoom=None):
    """Calculate appropriate resolution for bounds"""
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

def get_padding_for_zoom(zoom):
    """Get padding for zoom level to handle interpolation at edges"""
    if zoom >= 10:
        return 0.02
    elif zoom >= 8:
        return 0.08
    elif zoom >= 6:
        return 0.2
    elif zoom >= 4:
        return 0.4
    else:
        return 0.6

def pixel_to_lat_lng(x, y, bounds, tile_size=256):
    """Convert pixel coordinates to lat/lng"""
    lng = bounds['left'] + (x / tile_size) * (bounds['right'] - bounds['left'])
    lat = bounds['top'] - (y / tile_size) * (bounds['top'] - bounds['bottom'])
    return {'lat': lat, 'lng': lng}

def binary_search(arr, target):
    """Binary search implementation"""
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
    """Calculate alpha value based on RGB"""
    valid_colors = [c for c in [r, g, b] if c > 0]
    if len(valid_colors) > 0:
        return min(255, (sum(valid_colors) / len(valid_colors)) * 2)
    return 0

# Temperature conversion functions
def fahrenheit_to_celsius(fahrenheit):
    """Convert Fahrenheit to Celsius"""
    return ((fahrenheit - 32) * 5) / 9

def celsius_to_fahrenheit(celsius):
    """Convert Celsius to Fahrenheit"""
    return (celsius * 9) / 5 + 32