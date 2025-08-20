import logging
import xarray as xr

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TARGET_ZOOM_LEVELS = [6, 8, 10]

async def read_weather(local_netcdf_path):
  try:
    logger.info(f"Reading NetCDF file: {local_netcdf_path}")
    
    # Open NetCDF file with xarray
    ds = xr.open_dataset(local_netcdf_path)

    lats = ds['lat'].values
    lngs = ds['lon'].values

    logger.info(f"Latitude range: {lats.min():.3f} to {lats.max():.3f}")
    logger.info(f"Longitude range: {lngs.min():.3f} to {lngs.max():.3f}")

    max_zoom = max(TARGET_ZOOM_LEVELS)
    if max_zoom >= 10:
      sampling = 5
    elif max_zoom >= 8:
      sampling = 10  
    elif max_zoom >= 6:
      sampling = 20
    else:
      sampling = 50

    logger.info(f"Using sampling rate: {sampling} for max zoom {max_zoom}")

    sampled_ds = ds.isel(
      lat=slice(None, None, sampling),
      lon=slice(None, None, sampling),
      time=0
    )
    
    sampled_ds.rio.write_crs("EPSG:4326", inplace=True)
    sampled_ds = sampled_ds.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    
    logger.info(f"Sampled grid: {sampled_ds.dims}")
    logger.info(f"Variables: {list(sampled_ds.data_vars.keys())}")
    
    return sampled_ds

  except Exception as error:
    logger.error(f"Error reading NetCDF file: {error}")
    raise