import numpy as np
from typing import List, Dict, Optional
from bilinear_interpolation import interpolate_bilinear
from color_maps import (set_pixel_color, wind_speed_to_color,
  temperature_to_color, humidity_to_color, mc1_to_color, mc10_to_color,
  mc100_to_color, mc1000_to_color, mc_wood_to_color, mc_herb_to_color,
  kbdi_to_color, ic_to_color, erc_to_color, bi_to_color, sc_to_color,
  gsi_to_color
)
from utils import pixel_to_lat_lng

TILE_SIZE = 256

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