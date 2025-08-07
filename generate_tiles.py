import numpy as np
from typing import List, Dict, Optional
import logging
import io
from PIL import Image
from bilinear_interpolation import interpolate_bilinear
from color_maps import (set_pixel_color, wind_speed_to_color,
  temperature_to_color, humidity_to_color, mc1_to_color, mc10_to_color,
  mc100_to_color, mc1000_to_color, mc_wood_to_color, mc_herb_to_color,
  kbdi_to_color, ic_to_color, erc_to_color, bi_to_color, sc_to_color,
  gsi_to_color
)
from utils import convert_tile_to_coords, get_padding_for_zoom, pixel_to_lat_lng

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TILE_SIZE = 256

def generate_tile(zoom: int, x: int, y: int, data: List[Dict], 
                 variable: Optional[str] = None, forecast_hour: str = "01") -> Optional[bytes]:
    
    bounds = convert_tile_to_coords(zoom, x, y)
    
    # Add padding to get data beyond tile boundaries (helps with interpolation at edges)
    padding = get_padding_for_zoom(zoom)
    padded_bounds = {
        'left': bounds['left'] - padding,
        'right': bounds['right'] + padding,
        'top': bounds['top'] + padding,
        'bottom': bounds['bottom'] - padding
    }
    
    logger.info(f"Getting data for tile zoom {zoom}, x {x}, y {y}")
    
    # Filter data to padded bounds
    filtered_data = filter_data_to_bounds(data, padded_bounds, variable)
    
    if not filtered_data:
        logger.warning(f"No data found for tile {zoom}/{x}/{y}, variable {variable}")
        return None
    
    logger.info(f"Filtered to {len(filtered_data)} data points for tile {zoom}/{x}/{y}")
    
    # Create RGBA image array
    image_array = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
    
    logger.info(f"Filling tile pixels for zoom {zoom}, x {x}, y {y}")
    
    # Fill the raster with interpolated values
    fill_raster_with_interpolation(image_array, filtered_data, bounds, variable)
    
    logger.info(f"Saving tile image data for zoom {zoom}, x {x}, y {y}")
    
    # Convert NumPy array to PIL Image and return as PNG bytes
    image = Image.fromarray(image_array, 'RGBA')
    buffer = io.BytesIO()
    image.save(buffer, format='PNG')
    return buffer.getvalue()

def fill_raster_with_interpolation(pixels: np.ndarray, wind_data: List[Dict], 
                                 bounds: Dict, scalar: Optional[str] = None):
    
    # Build lookup structures for fast data access
    wind_map = {}
    lat_set = []
    lng_set = []
    
    for point in wind_data:
        key = f"{point['lat']},{point['lng']}"
        wind_map[key] = point
        lat_set.append(point['lat'])
        lng_set.append(point['lng'])
    
    # Remove duplicates and sort for binary search
    unique_lat_set = sorted(list(set(lat_set)))
    unique_lng_set = sorted(list(set(lng_set)))
    
    # Loop through each pixel in the tile
    for y in range(TILE_SIZE):
        for x in range(TILE_SIZE):
            # Convert pixel coordinates to lat/lng
            lat_lng = pixel_to_lat_lng(x, y, bounds, TILE_SIZE)
            
            if scalar:
                # Interpolate scalar value
                value = interpolate_bilinear(
                    lat_lng['lat'], lat_lng['lng'], 
                    wind_map, unique_lat_set, unique_lng_set, scalar
                )
                
                if value is not None:
                    # Map scalar to appropriate color function
                    if scalar == 'tmp':
                        color = temperature_to_color(value)
                    elif scalar == 'rh':
                        color = humidity_to_color(value)
                    elif scalar == 'mc1':
                        color = mc1_to_color(value)
                    elif scalar == 'mc10':
                        color = mc10_to_color(value)
                    elif scalar == 'mc100':
                        color = mc100_to_color(value)
                    elif scalar == 'mc1000':
                        color = mc1000_to_color(value)
                    elif scalar == 'mcwood':
                        color = mc_wood_to_color(value)
                    elif scalar == 'mcherb':
                        color = mc_herb_to_color(value)
                    elif scalar == 'kbdi':
                        color = kbdi_to_color(value)
                    elif scalar == 'ic':
                        color = ic_to_color(value)
                    elif scalar == 'erc':
                        color = erc_to_color(value)
                    elif scalar == 'bi':
                        color = bi_to_color(value)
                    elif scalar == 'sc':
                        color = sc_to_color(value)
                    elif scalar == 'gsi':
                        color = gsi_to_color(value)
                    else:
                        # Default fallback color mapping
                        from utils import alpha_color
                        color = {
                            'r': 0,
                            'g': 0, 
                            'b': int(value),
                            'a': alpha_color(0, 0, int(value))
                        }
                    
                    set_pixel_color(pixels, x, y, color)
                else:
                    # Transparent pixel for missing data
                    set_pixel_color(pixels, x, y, {'r': 0, 'g': 0, 'b': 0, 'a': 0})
            
            else:
                # Interpolate wind speed
                wind_speed = interpolate_bilinear(
                    lat_lng['lat'], lat_lng['lng'],
                    wind_map, unique_lat_set, unique_lng_set
                )
                
                if wind_speed is not None:
                    color = wind_speed_to_color(wind_speed)
                    set_pixel_color(pixels, x, y, color)
                else:
                    # Transparent pixel for missing data
                    set_pixel_color(pixels, x, y, {'r': 0, 'g': 0, 'b': 0, 'a': 0})

def filter_data_to_bounds(data: List[Dict], bounds: Dict, variable: Optional[str] = None) -> List[Dict]:

    filtered = []
    
    for point in data:
        # Check if point is within bounds
        lat = point.get('lat')
        lng = point.get('lng')
        
        if (lat is None or lng is None or
            lat < bounds['bottom'] or lat > bounds['top'] or
            lng < bounds['left'] or lng > bounds['right']):
            continue
            
        # If we're filtering for a specific variable, ensure it has a valid value
        if variable and variable != 'wspd':
            if point.get(variable) is None:
                continue
        elif not variable or variable == 'wspd':
            # For wind speed, we need u10 and v10 components
            if point.get('u10') is None or point.get('v10') is None:
                continue
                
        filtered.append(point)
    
    return filtered