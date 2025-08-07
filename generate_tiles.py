import numpy as np
from typing import List, Dict, Optional, Tuple
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

class WeatherDataProcessor:
    
    def __init__(self, data: List[Dict]):
        self.data = data
        self.wind_map = {}
        self.lat_set = []
        self.lng_set = []
        self._build_lookup_structures()
    
    def _build_lookup_structures(self):
        for point in self.data:
            key = f"{point['lat']},{point['lng']}"
            self.wind_map[key] = point
            self.lat_set.append(point['lat'])
            self.lng_set.append(point['lng'])
        
        # Remove duplicates and sort for binary search
        self.unique_lat_set = sorted(list(set(self.lat_set)))
        self.unique_lng_set = sorted(list(set(self.lng_set)))
        
        logger.info(f"Built lookup structures: {len(self.unique_lat_set)} unique lats, {len(self.unique_lng_set)} unique lngs")
    
    def filter_data_to_bounds(self, bounds: Dict, variable: Optional[str] = None) -> List[Dict]:
        # Use binary search to find lat/lng ranges instead of linear search
        lat_start_idx = max(0, self._find_lat_range_start(bounds['bottom']))
        lat_end_idx = min(len(self.unique_lat_set), self._find_lat_range_end(bounds['top']))
        
        lng_start_idx = max(0, self._find_lng_range_start(bounds['left']))
        lng_end_idx = min(len(self.unique_lng_set), self._find_lng_range_end(bounds['right']))
        
        filtered = []
        
        # Only check points within the lat/lng ranges
        for lat in self.unique_lat_set[lat_start_idx:lat_end_idx]:
            for lng in self.unique_lng_set[lng_start_idx:lng_end_idx]:
                point = self.wind_map.get(f"{lat},{lng}")
                if not point:
                    continue
                
                # Validate required data exists
                if variable and variable != 'wspd':
                    if point.get(variable) is None:
                        continue
                elif not variable or variable == 'wspd':
                    if point.get('u10') is None or point.get('v10') is None:
                        continue
                
                filtered.append(point)
        
        return filtered
    
    def _find_lat_range_start(self, min_lat: float) -> int:
        """Find starting index for latitude range"""
        for i, lat in enumerate(self.unique_lat_set):
            if lat >= min_lat:
                return max(0, i - 1)  # Include one point before for interpolation
        return len(self.unique_lat_set)
    
    def _find_lat_range_end(self, max_lat: float) -> int:
        """Find ending index for latitude range"""
        for i, lat in enumerate(self.unique_lat_set):
            if lat > max_lat:
                return min(len(self.unique_lat_set), i + 1)  # Include one point after
        return len(self.unique_lat_set)
    
    def _find_lng_range_start(self, min_lng: float) -> int:
        """Find starting index for longitude range"""
        for i, lng in enumerate(self.unique_lng_set):
            if lng >= min_lng:
                return max(0, i - 1)
        return len(self.unique_lng_set)
    
    def _find_lng_range_end(self, max_lng: float) -> int:
        """Find ending index for longitude range"""
        for i, lng in enumerate(self.unique_lng_set):
            if lng > max_lng:
                return min(len(self.unique_lng_set), i + 1)
        return len(self.unique_lng_set)

def generate_tile_optimized(zoom: int, x: int, y: int, processor: WeatherDataProcessor, 
                          variable: Optional[str] = None, forecast_hour: str = "01") -> Optional[bytes]:

    bounds = convert_tile_to_coords(zoom, x, y)
    
    # Add padding to get data beyond tile boundaries
    padding = get_padding_for_zoom(zoom)
    padded_bounds = {
        'left': bounds['left'] - padding,
        'right': bounds['right'] + padding,
        'top': bounds['top'] + padding,
        'bottom': bounds['bottom'] - padding
    }
    
    # Use optimized filtering
    filtered_data = processor.filter_data_to_bounds(padded_bounds, variable)
    
    if not filtered_data:
        return None
    
    # Create RGBA image array
    image_array = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
    
    # Fill the raster with interpolated values using pre-built structures
    fill_raster_optimized(image_array, filtered_data, bounds, variable, processor)
    
    # Convert NumPy array to PIL Image and return as PNG bytes
    image = Image.fromarray(image_array, 'RGBA')
    buffer = io.BytesIO()
    image.save(buffer, format='PNG')
    return buffer.getvalue()

def fill_raster_optimized(pixels: np.ndarray, wind_data: List[Dict], 
                         bounds: Dict, variable: Optional[str], processor: WeatherDataProcessor):
    
    # Use pre-built lookup structures from processor
    wind_map = processor.wind_map
    unique_lat_set = processor.unique_lat_set
    unique_lng_set = processor.unique_lng_set
    
    # Loop through each pixel in the tile
    for y in range(TILE_SIZE):
        for x in range(TILE_SIZE):
            # Convert pixel coordinates to lat/lng
            lat_lng = pixel_to_lat_lng(x, y, bounds, TILE_SIZE)
            
            # Interpolate value using pre-built structures
            value = interpolate_bilinear(
                lat_lng['lat'], lat_lng['lng'], 
                wind_map, unique_lat_set, unique_lng_set, variable
            )
            
            if value is not None:
                # Map to appropriate color function
                color = get_color_for_variable(variable, value)
                set_pixel_color(pixels, x, y, color)
            else:
                # Transparent pixel for missing data
                set_pixel_color(pixels, x, y, {'r': 0, 'g': 0, 'b': 0, 'a': 0})

def get_color_for_variable(variable: Optional[str], value: float) -> Dict[str, int]:

    if not variable or variable == 'wspd':
        return wind_speed_to_color(value)
    elif variable == 'tmp':
        return temperature_to_color(value)
    elif variable == 'rh':
        return humidity_to_color(value)
    elif variable == 'mc1':
        return mc1_to_color(value)
    elif variable == 'mc10':
        return mc10_to_color(value)
    elif variable == 'mc100':
        return mc100_to_color(value)
    elif variable == 'mc1000':
        return mc1000_to_color(value)
    elif variable == 'mcwood':
        return mc_wood_to_color(value)
    elif variable == 'mcherb':
        return mc_herb_to_color(value)
    elif variable == 'kbdi':
        return kbdi_to_color(value)
    elif variable == 'ic':
        return ic_to_color(value)
    elif variable == 'erc':
        return erc_to_color(value)
    elif variable == 'bi':
        return bi_to_color(value)
    elif variable == 'sc':
        return sc_to_color(value)
    elif variable == 'gsi':
        return gsi_to_color(value)
    else:
        # Default fallback color mapping
        from utils import alpha_color
        return {
            'r': 0,
            'g': 0, 
            'b': int(min(255, max(0, value))),
            'a': alpha_color(0, 0, int(min(255, max(0, value))))
        }

# Keep the original function for backward compatibility
def generate_tile(zoom: int, x: int, y: int, data: List[Dict], 
                 variable: Optional[str] = None, forecast_hour: str = "01") -> Optional[bytes]:

    processor = WeatherDataProcessor(data)
    return generate_tile_optimized(zoom, x, y, processor, variable, forecast_hour)