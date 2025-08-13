import logging
import xarray as xr
import numpy as np

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TARGET_ZOOM_LEVELS = [6, 8, 10]

async def read_weather(local_netcdf_path):
  try:
    logger.info(f"Reading NetCDF file: {local_netcdf_path}")
    
    # Open NetCDF file with xarray
    ds = xr.open_dataset(local_netcdf_path)

    lats = ds['lat'].values
    lons = ds['lon'].values

    logger.info(f"Latitude range: {lats.min():.3f} to {lats.max():.3f}")
    logger.info(f"Longitude range: {lons.min():.3f} to {lons.max():.3f}")

    max_zoom = max(TARGET_ZOOM_LEVELS)
    if max_zoom >= 10:
      sampling = 5
    elif max_zoom >= 8:
      sampling = 10  
    elif max_zoom >= 6:
      sampling = 20
    else:
      sampling = 50

    logger.info(f"Using sampling rate: ::{sampling} for max zoom {max_zoom}")

    lat_indices = np.arange(0, len(lats), sampling)
    lon_indices = np.arange(0, len(lons), sampling)
    
    lat_sample = lats[lat_indices]
    lon_sample = lons[lon_indices]

    logger.info(f"Sample grid size: {len(lat_sample)} x {len(lon_sample)} = {len(lat_sample) * len(lon_sample)} points")
    
    lat_grid, lon_grid = np.meshgrid(lat_sample, lon_sample, indexing='ij')
    lat_idx_grid, lon_idx_grid = np.meshgrid(lat_indices, lon_indices, indexing='ij')
    
    # Flatten the grids for easier processing
    flat_lats = lat_grid.flatten()
    flat_lons = lon_grid.flatten()
    flat_lat_indices = lat_idx_grid.flatten()
    flat_lon_indices = lon_idx_grid.flatten()
    
    logger.info("Extracting all variables using vectorized operations...")
    
    # Extract all data variables at once using vectorized indexing
    data_dict = {}
    
    for var_name in ds.data_vars:
      try:
        logger.info(f"Extracting variable: {var_name}")
        
        # Get the variable data for time=0
        var_data = ds[var_name].isel(time=0)
        
        # Extract values for all sample points at once using advanced indexing
        values = var_data.values[flat_lat_indices, flat_lon_indices]
        
        # Store in dictionary
        data_dict[var_name.lower()] = values
          
      except Exception as e:
        logger.warning(f"Could not extract {var_name}: {e}")
        # Fill with None values for missing variables
        data_dict[var_name.lower()] = np.full(len(flat_lats), None)
  
    ds.close()
    
    logger.info("Building data points list...")
    
    # Build the final data points list
    data_points = []
    for i in range(len(flat_lats)):
      point = {
        'lat': float(flat_lats[i]),
        'lng': float(flat_lons[i])
      }
      
      # Add all variables
      for var_name, values in data_dict.items():
        value = values[i]
        # Handle NaN values
        if isinstance(value, np.ndarray):
          value = value.item()
        
        if np.isnan(value) if isinstance(value, (float, np.floating)) else False:
          point[var_name] = None
        else:
          point[var_name] = float(value) if value is not None else None
      
      data_points.append(point)
    
    logger.info(f"Extracted {len(data_points)} data points")
    if data_points:
      logger.info(f"Sample point: {data_points[0]}")
    
    return data_points

  except Exception as error:
    logger.error(f"Error reading NetCDF file: {error}")
    raise