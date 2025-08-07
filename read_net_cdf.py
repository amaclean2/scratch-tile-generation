import logging
import xarray as xr
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def read_weather(local_netcdf_path):
  try:
    logger.info(f"Reading NetCDF file: {local_netcdf_path}")
    
    # Open NetCDF file with xarray
    ds = xr.open_dataset(local_netcdf_path)

    lats = ds['lat'].values
    lons = ds['lon'].values

    logger.info(f"Latitude range: {lats.min():.3f} to {lats.max():.3f}")
    logger.info(f"Longitude range: {lons.min():.3f} to {lons.max():.3f}")

    lat_sample = lats[::10]  
    lon_sample = lons[::10]

    logger.info(f"Sample size: {len(lat_sample)} x {len(lon_sample)} = {len(lat_sample) * len(lon_sample)} points")
    
    data_points = []
    for i, lat in enumerate(lat_sample):
      for j, lon in enumerate(lon_sample):
        # Get actual indices in the full arrays
        lat_idx = i * 10
        lon_idx = j * 10
        
        # Extract values for this point (time=0 since we have single time step)
        point = {
          'lat': float(lat),
          'lng': float(lon),  # Note: using 'lng' to match your original format
        }
        
        # Add all the variables
        for var_name in ds.data_vars:
          try:
            value = ds[var_name].isel(time=0, lat=lat_idx, lon=lon_idx).values
            # Handle NaN values
            if np.isnan(value):
                point[var_name.lower()] = None
            else:
                point[var_name.lower()] = float(value)
          except Exception as e:
            logger.warning(f"Could not extract {var_name}: {e}")
            point[var_name.lower()] = None
        
        data_points.append(point)
      
    ds.close()
    logger.info(f"Extracted {len(data_points)} data points")
    logger.info(f"Sample point: {data_points[0] if data_points else 'None'}")
    
    return data_points
  
  except Exception as error:
    logger.error(f"Error reading NetCDF file: {error}")
    raise