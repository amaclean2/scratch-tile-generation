import numpy as np
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
        
def apply_wind_colors(arr):
  rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
  breakpoints = np.array([0, 0.5, 1, 2, 3, 4.5, 6, 7.5, 9, 10.5, 12, 13.5, 15, 17.5, 20, 999])
  colors = np.array([
    [0, 0, 0, 0],           # 0
    [37, 72, 113, 90],      # 0.5
    [74, 144, 226, 180],    # 1
    [77, 172, 173, 190],    # 2  
    [80, 200, 120, 200],    # 3
    [167, 217, 89, 210],    # 4.5
    [255, 235, 59, 220],    # 6
    [255, 201, 48, 230],    # 7.5
    [255, 167, 38, 240],    # 9
    [249, 115, 46, 247],    # 10.5
    [244, 67, 54, 255],     # 12
    [200, 53, 115, 255],    # 13.5
    [156, 39, 176, 255],    # 15
    [94, 36, 104, 255],     # 17.5
    [33, 33, 33, 255],      # 20
    [33, 33, 33, 255]       # 999
  ])
  
  indices = np.digitize(arr, breakpoints) - 1
  indices = np.clip(indices, 0, len(colors) - 1)
  
  rgba[:,:,0] = colors[indices, 0]
  rgba[:,:,1] = colors[indices, 1]
  rgba[:,:,2] = colors[indices, 2]
  rgba[:,:,3] = colors[indices, 3]
  
  return rgba

def apply_temperature_colors(arr):
  rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
  
  breakpoints = np.array([0, 20, 32, 40, 50, 55, 60, 65, 70, 75, 80, 82, 85, 88, 90, 95, 100, 110, 999])
  
  colors = np.array([
    [4, 26, 64, 255],      # < 20°F: Deepest blue (arctic)
    [8, 48, 107, 255],     # 20-32°F: Very dark blue (bitter cold)
    [25, 57, 138, 255],    # 32-40°F: Dark blue (freezing)
    [49, 54, 149, 255],    # 40-50°F: Blue (very cold)
    [54, 75, 154, 255],    # 50-55°F: Medium blue (cold)
    [69, 117, 180, 255],   # 55-60°F: Blue (cool)
    [94, 142, 191, 255],   # 60-65°F: Light blue (brisk)
    [116, 173, 209, 240],  # 65-70°F: Light blue (mild cool)
    [142, 190, 220, 230],  # 70-75°F: Very light blue (pleasant)
    [171, 217, 233, 220],  # 75-80°F: Pale blue (comfortable)
    [235, 235, 180, 210],  # 80-82°F: Light yellow-green (nice)
    [255, 255, 204, 200],  # 82-85°F: Yellow (warm)
    [255, 245, 157, 210],  # 85-88°F: Bright yellow (getting warm)
    [254, 224, 144, 220],  # 88-90°F: Orange-yellow (hot)
    [253, 204, 138, 230],  # 90-95°F: Light orange (very hot)
    [253, 174, 97, 240],   # 95-100°F: Orange (extremely hot)
    [244, 109, 67, 255],   # 100-110°F: Red-orange (dangerous)
    [215, 48, 39, 255],    # 110°F+: Red (extreme danger)
    [165, 0, 38, 255]      # Fallback: Dark red
  ])
  
  indices = np.digitize(arr, breakpoints) - 1
  indices = np.clip(indices, 0, len(colors) - 1)

  rgba[:,:,0] = colors[indices, 0]
  rgba[:,:,1] = colors[indices, 1]
  rgba[:,:,2] = colors[indices, 2]
  rgba[:,:,3] = colors[indices, 3]
  
  return rgba

def apply_humidity_colors(arr):
  rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
  
  arr_clean = np.where(np.isnan(arr), 0, arr)
  
  breakpoints = np.array([0, 5, 10, 15, 20, 25, 30, 40, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100])
  
  colors = np.array([
    [101, 37, 6, 255],     # 0-5%: Deep brown (desert dry)
    [140, 81, 10, 255],    # 5-10%: Brown (very dry)
    [166, 108, 27, 250],   # 10-15%: Dark tan (dry)
    [191, 129, 45, 240],   # 15-20%: Orange-brown (low humidity)
    [210, 154, 69, 235],   # 20-25%: Light brown (still dry)
    [223, 194, 125, 220],  # 25-30%: Tan (getting better)
    [235, 210, 149, 210],  # 30-40%: Light tan (low-moderate)
    [246, 232, 195, 200],  # 40-50%: Very light tan (moderate)
    [248, 240, 210, 190],  # 50-55%: Off-white (comfortable)
    [245, 245, 245, 180],  # 55-60%: White (good)
    [230, 242, 240, 190],  # 60-65%: Very pale green (nice)
    [199, 234, 229, 200],  # 65-70%: Light blue-green (humid)
    [170, 220, 210, 210],  # 70-75%: Blue-green (quite humid)
    [128, 205, 193, 220],  # 75-80%: Blue-green (very humid)
    [85, 180, 170, 230],   # 80-85%: Teal (high humidity)
    [53, 151, 143, 240],   # 85-90%: Dark teal (very high)
    [25, 120, 115, 250],   # 90-95%: Deep teal (extremely humid)
    [1, 102, 94, 255],     # 95-100%: Very dark teal (saturated)
    [0, 60, 48, 255]       # 100%: Darkest teal (max humidity)
  ])
  
  indices = np.digitize(arr_clean, breakpoints) - 1
  indices = np.clip(indices, 0, len(colors) - 1)

  rgba[:,:,0] = colors[indices, 0]
  rgba[:,:,1] = colors[indices, 1]
  rgba[:,:,2] = colors[indices, 2]
  rgba[:,:,3] = colors[indices, 3]
  
  return rgba

def apply_fuel_moisture_colors(arr, max_moisture=30):
  rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
  
  arr_clean = np.where(np.isnan(arr), 0, arr)
  ratio = np.clip(arr_clean / max_moisture, 0, 1)
  
  rgba[:,:,0] = (ratio * 255).astype(np.uint8)  # Red channel
  rgba[:,:,1] = ((1 - ratio) * 255).astype(np.uint8)  # Green channel
  rgba[:,:,2] = (0 * np.ones_like(ratio)).astype(np.uint8)  # Blue channel
  rgba[:,:,3] = 255  # Alpha channel
  
  return rgba

def apply_mc1_colors(arr):
  return apply_fuel_moisture_colors(arr, max_moisture=40)

def apply_mc10_colors(arr):
  return apply_fuel_moisture_colors(arr, max_moisture=40)

def apply_mc100_colors(arr):
  return apply_fuel_moisture_colors(arr, max_moisture=40)

def apply_mc1000_colors(arr):
  return apply_fuel_moisture_colors(arr, max_moisture=40)

def apply_mc_wood_colors(arr):
  return apply_fuel_moisture_colors(arr, max_moisture=200)

def apply_mc_herb_colors(arr):
  return apply_fuel_moisture_colors(arr, max_moisture=250)

def apply_fire_danger_colors(arr, max_value=100):
  rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
  
  arr_clean = np.where(np.isnan(arr), 0, arr)
  ratio = np.clip(arr_clean / max_value, 0, 1)
  
  rgba[:,:,0] = (ratio * 255).astype(np.uint8)  # Red channel
  rgba[:,:,1] = ((1 - ratio) * 255).astype(np.uint8)  # Green channel
  rgba[:,:,2] = (0 * np.ones_like(ratio)).astype(np.uint8)  # Blue channel
  rgba[:,:,3] = 255  # Alpha channel
  
  return rgba

def apply_ic_colors(arr):
  return apply_fire_danger_colors(arr, max_value=100)

def apply_erc_colors(arr):
  return apply_fire_danger_colors(arr, max_value=100)

def apply_bi_colors(arr):
  return apply_fire_danger_colors(arr, max_value=200)

def apply_sc_colors(arr):
  return apply_fire_danger_colors(arr, max_value=100)

def apply_kbdi_colors(arr):
  rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
  
  arr_clean = np.where(np.isnan(arr), 0, arr)
  ratio = np.clip(arr_clean / 700.0, 0, 1)
  
  rgba[:,:,0] = (ratio * 255).astype(np.uint8)  # Red channel
  rgba[:,:,1] = ((1 - ratio) * 255).astype(np.uint8)  # Green channel
  rgba[:,:,2] = (0 * np.ones_like(ratio)).astype(np.uint8)  # Blue channel
  rgba[:,:,3] = 255  # Alpha channel
  
  return rgba

def apply_gsi_colors(arr):
  rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
  
  arr_clean = np.where(np.isnan(arr), 0, arr)
  
  breakpoints = np.array([0, 0.2, 0.4, 0.6, 0.8, 0.9, 1])
  colors = np.array([
    [139, 69, 19, 255],
    [205, 133, 63, 240],
    [245, 222, 179, 220],
    [255, 250, 205, 200],
    [173, 255, 47, 220],
    [50, 205, 50, 240],
    [0, 100, 0, 255]
  ])
  
  indicies = np.digitize(arr_clean / (len(breakpoints) - 1), breakpoints) - 1
  indicies = np.clip(indicies, 0, len(colors) - 1)
  
  rgba[:,:,0] = colors[indicies, 0]
  rgba[:,:,1] = colors[indicies, 1]
  rgba[:,:,2] = colors[indicies, 2]
  rgba[:,:,3] = colors[indicies, 3]
  
  return rgba