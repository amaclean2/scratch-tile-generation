import numpy as np
        
def apply_wind_colors(arr):
    rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
    breakpoints = np.array([0, 1, 3, 6, 9, 12, 15, 20, 999])
    colors = np.array([
        [0, 0, 0, 0],
        [74, 144, 226, 180],
        [80, 200, 120, 200],
        [255, 235, 59, 220],
        [255, 167, 38, 240],
        [244, 67, 54, 255],
        [156, 39, 176, 255],
        [33, 33, 33, 255]
    ])
    
    indicies = np.digitize(arr, breakpoints) - 1
    indicies = np.clip(indicies, 0, len(colors) - 1)
    
    rgba[:,:,0] = colors[indicies, 0]
    rgba[:,:,1] = colors[indicies, 1]
    rgba[:,:,2] = colors[indicies, 2]
    rgba[:,:,3] = colors[indicies, 3]
    
    return rgba

def apply_temperature_colors(arr):
    rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
    breakpoints = np.array([-20, -5, 5, 15, 25, 30, 35, 40, 999])
    
    colors = np.array([
        [49, 54, 149, 255],
        [69, 117, 180, 255],
        [116, 173, 209, 240],
        [171, 217, 233, 220],
        [255, 255, 204, 200],
        [254, 224, 144, 220],
        [253, 174, 97, 240],
        [244, 109, 67, 255],
        [165, 0, 38, 255]
    ])
    
    indicies = np.digitize(arr, breakpoints) - 1
    indicies = np.clip(indicies, 0, len(colors) - 1)
    
    rgba[:,:,0] = colors[indicies, 0]
    rgba[:,:,1] = colors[indicies, 1]
    rgba[:,:,2] = colors[indicies, 2]
    rgba[:,:,3] = colors[indicies, 3]
    
    return rgba

def apply_humidity_colors(arr):
    rgba = np.zeros((*arr.shape, 4), dtype=np.uint8)
    
    arr_clean = np.where(np.isnan(arr), 0, arr)
    
    breakpoints = np.array([0, 10, 20, 35, 45, 55, 65, 75, 85, 95, 100])
    colors = np.array([
        [140, 81, 10, 255],
        [191, 129, 45, 240],
        [223, 194, 125, 220],
        [246, 232, 195, 200],
        [245, 245, 245, 180],
        [199, 234, 229, 200],
        [128, 205, 193, 220],
        [53, 151, 143, 240],
        [1, 102, 94, 255],
        [0, 60, 48, 255]
    ])
    
    indicies = np.digitize(arr_clean / 100.0 * (len(breakpoints) - 1), breakpoints) - 1
    indicies = np.clip(indicies, 0, len(colors) - 1)
    
    rgba[:,:,0] = colors[indicies, 0]
    rgba[:,:,1] = colors[indicies, 1]
    rgba[:,:,2] = colors[indicies, 2]
    rgba[:,:,3] = colors[indicies, 3]
    
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