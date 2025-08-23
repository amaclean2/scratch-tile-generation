import numpy as np
import mercantile
import logging
import io
from PIL import Image
from color_maps import (apply_wind_colors,
  apply_temperature_colors, apply_humidity_colors, apply_mc1_colors, apply_mc10_colors,
  apply_mc100_colors, apply_mc1000_colors, apply_mc_wood_colors, apply_mc_herb_colors,
  apply_kbdi_colors, apply_ic_colors, apply_erc_colors, apply_bi_colors, apply_sc_colors,
  apply_gsi_colors
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TILE_SIZE = 256

def generate_tile(x, y, zoom, variable, ds):
  try:
    bounds = mercantile.bounds(x, y, zoom)
    
    if variable not in ds.data_vars:
      logger.warning(f"Variable {variable} not found in dataset")
      return None
    
    var_data = ds[variable].rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    
    tile_ds = var_data.rio.clip_box(
			bounds.west, bounds.south,
			bounds.east, bounds.north,
      allow_one_dimensional_raster=True
		)
    
    if tile_ds.size == 0:
      logger.debug(f"No data in tile bouds for {zoom}/{x}/{y}")
      return None
    
    tile_ds = tile_ds.rio.reproject(
			var_data.rio.crs,
			shape=(TILE_SIZE, TILE_SIZE),
			resampling=3
		)
    
    arr = tile_ds.values
    
    if np.isnan(arr).all():
      logger.debug(f"All NaN values in tile {zoom}/{x}/{y}")
      return None
    
    img_arr = apply_vectorized_colors(arr, variable)
    
    image = Image.fromarray(img_arr)
    
    buffer = io.BytesIO()
    image.save(buffer, format="PNG", optimize=True, compress_level=1)
    return buffer.getvalue()
  
  except Exception as e:
    logger.warning(f"failed to generate tile {zoom}/{x}/{y}: {e}")
    return None
  
def apply_vectorized_colors(arr, variable):
  rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
  
  arr_clean = np.where(np.isnan(arr), 0, arr)
  
  match variable:
    case 'wspd' | None:
      return apply_wind_colors(arr_clean)
    case 'tmp':
      return apply_temperature_colors(arr_clean)
    case 'rh':
      return apply_humidity_colors(arr_clean)
    case 'MC1':
      return apply_mc1_colors(arr_clean)
    case 'MC10':
      return apply_mc10_colors(arr_clean)
    case 'MC100':
      return apply_mc100_colors(arr_clean)
    case 'MC1000':
      return apply_mc1000_colors(arr_clean)
    case 'MCWOOD':
      return apply_mc_wood_colors(arr_clean)
    case 'MCHERB':
      return apply_mc_herb_colors(arr_clean)
    case 'KBDI':
      return apply_kbdi_colors(arr_clean)
    case 'IC':
      return apply_ic_colors(arr_clean)
    case 'ERC':
      return apply_erc_colors(arr_clean)
    case 'BI':
      return apply_bi_colors(arr_clean)
    case 'SC':
      return apply_sc_colors(arr_clean)
    case 'GSI':
      return apply_gsi_colors(arr_clean)
    case _:
      normalized = np.clip(arr_clean / 100.0, 0, 1)
      rgba[:, :, 0] = (normalized * 255).astype(np.uint8)
      rgba[:, :, 1] = (normalized * 255).astype(np.uint8)
      rgba[:, :, 2] = (normalized * 255).astype(np.uint8)
      rgba[:, :, 3] = 255
      return rgba