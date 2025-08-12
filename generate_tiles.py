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

"""
Weather Data Proccessor takes the full dataset and organizes in a
sorted lat set, lng set and a map to be able to quickly access data
"""
class WeatherDataProcessor:
    
	def __init__(self, data: List[Dict]):
		self.data = data
		self.data_map = {}
		self.lat_set = []
		self.lng_set = []
		self._build_lookup_structures()

	# this is probably the most time intensive function
	def _build_lookup_structures(self):
		for point in self.data:
			key = f"{point['lat']},{point['lng']}"
			self.data_map[key] = point
			self.lat_set.append(point['lat'])
			self.lng_set.append(point['lng'])
		
		# Remove duplicates and sort for binary search
		self.unique_lat_set = sorted(list(set(self.lat_set)))
		self.unique_lng_set = sorted(list(set(self.lng_set)))
		
		logger.info(f"Built lookup structures: {len(self.unique_lat_set)} unique lats, {len(self.unique_lng_set)} unique lngs")

	# this is supposed to run in O(n) time
	def get_data_within_bounds(self, bounds: Dict, variable: Optional[str] = None) -> List[Dict]:
		lat_start_idx = max(0, self._find_lat_range_start(bounds['bottom']))
		lat_end_idx = min(len(self.unique_lat_set), self._find_lat_range_end(bounds['top']))
		
		lng_start_idx = max(0, self._find_lng_range_start(bounds['left']))
		lng_end_idx = min(len(self.unique_lng_set), self._find_lng_range_end(bounds['right']))
		
		filtered = []
		
		# Only check points within the lat/lng ranges
		for lat in self.unique_lat_set[lat_start_idx:lat_end_idx]:
			for lng in self.unique_lng_set[lng_start_idx:lng_end_idx]:
				point = self.data_map.get(f"{lat},{lng}")
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

	# helper functions to find the start and end indices for lat/lng ranges
	def _find_lat_range_start(self, min_lat: float) -> int:
		for i, lat in enumerate(self.unique_lat_set):
			if lat >= min_lat:
				return max(0, i - 1)
		return len(self.unique_lat_set)

	def _find_lat_range_end(self, max_lat: float) -> int:
		for i, lat in enumerate(self.unique_lat_set):
			if lat > max_lat:
				return min(len(self.unique_lat_set), i + 1)
		return len(self.unique_lat_set)

	def _find_lng_range_start(self, min_lng: float) -> int:
		for i, lng in enumerate(self.unique_lng_set):
			if lng >= min_lng:
				return max(0, i - 1)
		return len(self.unique_lng_set)

	def _find_lng_range_end(self, max_lng: float) -> int:
		for i, lng in enumerate(self.unique_lng_set):
			if lng > max_lng:
				return min(len(self.unique_lng_set), i + 1)
		return len(self.unique_lng_set)

def generate_tile(zoom, x, y, data, variable):
	processor = WeatherDataProcessor(data)
	
	bounds = convert_tile_to_coords(zoom, x, y)
	
	padding = get_padding_for_zoom(zoom)
	padded_bounds = {
		'left': bounds['left'] - padding,
		'right': bounds['right'] + padding,
		'top': bounds['top'] + padding,
		'bottom': bounds['bottom'] - padding
	}
	
	bounded_data = processor.get_data_within_bounds(padded_bounds, variable)
	
	if not bounded_data:
		return None
	
	image_array = np.zeros((TILE_SIZE, TILE_SIZE, 4), dtype=np.uint8)
	
	fill_raster(image_array, bounded_data, bounds, variable, processor)
	
	image = Image.fromarray(image_array, 'RGBA')
	buffer = io.BytesIO()
	image.save(buffer, format='PNG')
	return buffer.getvalue()


def fill_raster(pixels: np.ndarray, wind_data: List[Dict], 
   bounds: Dict, variable: Optional[str], processor: WeatherDataProcessor):
    
	data_map = processor.data_map
	unique_lat_set = processor.unique_lat_set
	unique_lng_set = processor.unique_lng_set
	
	for y in range(TILE_SIZE):
		for x in range(TILE_SIZE):
			lat_lng = pixel_to_lat_lng(x, y, bounds, TILE_SIZE)
			
			value = interpolate_bilinear(
				lat_lng['lat'], lat_lng['lng'], 
				data_map, unique_lat_set, unique_lng_set, variable
			)
			
			if value is not None:
				color = get_color_for_variable(variable, value)
				set_pixel_color(pixels, x, y, color)
			else:
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
		from utils import alpha_color
		return {
			'r': 0,
			'g': 0, 
			'b': int(min(255, max(0, value))),
			'a': alpha_color(0, 0, int(min(255, max(0, value))))
		}