import math
from typing import Dict, List, Optional
from utils import binary_search

def interpolate_bilinear(lat: float, lng: float, wind_map: Dict[str, Dict], 
                        lat_set: List[float], lng_set: List[float], 
                        scalar: Optional[str] = None) -> Optional[float]:
    
    # Truncate input coordinates to 4 decimal places for consistency
    def truncate(num: float) -> float:
        return math.trunc(num * 10000) / 10000
    
    lat_trunc = truncate(lat)
    lng_trunc = truncate(lng)
    
    # Find insertion points using binary search
    lat_insert_idx = binary_search(lat_set, lat_trunc)
    lng_insert_idx = binary_search(lng_set, lng_trunc)
    
    # Handle edge cases with extrapolation instead of rejection
    # Latitude bounds
    if lat_insert_idx == 0:
        # Use first two points for extrapolation
        lat_lower_idx = 0
        lat_upper_idx = 1

    elif lat_insert_idx >= len(lat_set):
        # Use last two points for extrapolation  
        lat_lower_idx = len(lat_set) - 2
        lat_upper_idx = len(lat_set) - 1
        
    else:
        # Normal interpolation
        lat_lower_idx = lat_insert_idx - 1
        lat_upper_idx = lat_insert_idx
    
    # Longitude bounds
    if lng_insert_idx == 0:
        lng_lower_idx = 0
        lng_upper_idx = 1
    elif lng_insert_idx >= len(lng_set):
        lng_lower_idx = len(lng_set) - 2
        lng_upper_idx = len(lng_set) - 1
    else:
        lng_lower_idx = lng_insert_idx - 1
        lng_upper_idx = lng_insert_idx
    
    # Verify we have enough data points
    if lat_upper_idx >= len(lat_set) or lng_upper_idx >= len(lng_set):
        return None
    
    # Get the four corner coordinates
    lat_below = lat_set[lat_lower_idx]
    lat_above = lat_set[lat_upper_idx]
    lng_left = lng_set[lng_lower_idx]
    lng_right = lng_set[lng_upper_idx]
    
    # Look up the four corner data points
    bottom_left = wind_map.get(f"{lat_below},{lng_left}")
    bottom_right = wind_map.get(f"{lat_below},{lng_right}")
    top_left = wind_map.get(f"{lat_above},{lng_left}")
    top_right = wind_map.get(f"{lat_above},{lng_right}")
    
    # All four points must exist
    if not all([bottom_left, bottom_right, top_left, top_right]):
        return None
    
    # Truncate bounding values for calculation
    lat_below_trunc = truncate(lat_below)
    lat_above_trunc = truncate(lat_above)
    lng_left_trunc = truncate(lng_left)
    lng_right_trunc = truncate(lng_right)
    
    # Calculate interpolation weights
    if lng_right_trunc == lng_left_trunc:
        x = 0
    else:
        x = (lng_trunc - lng_left_trunc) / (lng_right_trunc - lng_left_trunc)
    
    if lat_above_trunc == lat_below_trunc:
        y = 0
    else:
        y = (lat_trunc - lat_below_trunc) / (lat_above_trunc - lat_below_trunc)
    
    def establish_value(point: Dict) -> float:
        """Extract the value to interpolate from a data point"""
        if scalar:
            return float(point.get(scalar, 0))
        else:
            # Calculate wind speed from u10 and v10 components
            u10 = point.get('u10', 0)
            v10 = point.get('v10', 0)
            return math.sqrt(u10 ** 2 + v10 ** 2)
    
    # Perform bilinear interpolation
    bottom_value = establish_value(bottom_left) * (1 - x) + establish_value(bottom_right) * x
    top_value = establish_value(top_left) * (1 - x) + establish_value(top_right) * x
    
    final_value = bottom_value * (1 - y) + top_value * y
    
    # Truncate final result to 4 decimal places
    return truncate(final_value)