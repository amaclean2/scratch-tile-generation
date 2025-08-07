from utils import fahrenheit_to_celsius

def wind_speed_to_color(speed):
    if speed < 1:
        return {'r': 0, 'g': 0, 'b': 0, 'a': 0}
    elif speed < 3:
        return {'r': 74, 'g': 144, 'b': 226, 'a': 180}
    elif speed < 6:
        return {'r': 80, 'g': 200, 'b': 120, 'a': 200}
    elif speed < 9:
        return {'r': 255, 'g': 235, 'b': 59, 'a': 220}
    elif speed < 12:
        return {'r': 255, 'g': 167, 'b': 38, 'a': 240}
    elif speed < 15:
        return {'r': 244, 'g': 67, 'b': 54, 'a': 255}
    elif speed < 20:
        return {'r': 156, 'g': 39, 'b': 176, 'a': 255}
    else:
        return {'r': 33, 'g': 33, 'b': 33, 'a': 255}

def temperature_to_color(temp):
    temp = fahrenheit_to_celsius(temp)
    if temp < -20:
        return {'r': 49, 'g': 54, 'b': 149, 'a': 255}
    elif temp < -5:
        return {'r': 69, 'g': 117, 'b': 180, 'a': 255}
    elif temp < 5:
        return {'r': 116, 'g': 173, 'b': 209, 'a': 240}
    elif temp < 15:
        return {'r': 171, 'g': 217, 'b': 233, 'a': 220}
    elif temp < 25:
        return {'r': 255, 'g': 255, 'b': 204, 'a': 200}
    elif temp < 30:
        return {'r': 254, 'g': 224, 'b': 144, 'a': 220}
    elif temp < 35:
        return {'r': 253, 'g': 174, 'b': 97, 'a': 240}
    elif temp < 40:
        return {'r': 244, 'g': 109, 'b': 67, 'a': 255}
    else:
        return {'r': 165, 'g': 0, 'b': 38, 'a': 255}

def humidity_to_color(rh_percent):
    if rh_percent < 10:
        return {'r': 140, 'g': 81, 'b': 10, 'a': 255}
    elif rh_percent < 20:
        return {'r': 191, 'g': 129, 'b': 45, 'a': 240}
    elif rh_percent < 35:
        return {'r': 223, 'g': 194, 'b': 125, 'a': 220}
    elif rh_percent < 45:
        return {'r': 246, 'g': 232, 'b': 195, 'a': 200}
    elif rh_percent < 55:
        return {'r': 245, 'g': 245, 'b': 245, 'a': 180}
    elif rh_percent < 65:
        return {'r': 199, 'g': 234, 'b': 229, 'a': 200}
    elif rh_percent < 75:
        return {'r': 128, 'g': 205, 'b': 193, 'a': 220}
    elif rh_percent < 85:
        return {'r': 53, 'g': 151, 'b': 143, 'a': 240}
    elif rh_percent < 95:
        return {'r': 1, 'g': 102, 'b': 94, 'a': 255}
    else:
        return {'r': 0, 'g': 60, 'b': 48, 'a': 255}

def fuel_moisture_to_color(moisture_percent, max_moisture=30):
    ratio = min(moisture_percent / max_moisture, 1)
    
    if ratio < 0.1:
        return {'r': 139, 'g': 69, 'b': 19, 'a': 255}
    elif ratio < 0.2:
        return {'r': 205, 'g': 133, 'b': 63, 'a': 255}
    elif ratio < 0.3:
        return {'r': 222, 'g': 184, 'b': 135, 'a': 240}
    elif ratio < 0.4:
        return {'r': 245, 'g': 222, 'b': 179, 'a': 220}
    elif ratio < 0.5:
        return {'r': 173, 'g': 255, 'b': 47, 'a': 200}
    elif ratio < 0.7:
        return {'r': 50, 'g': 205, 'b': 50, 'a': 220}
    elif ratio < 0.9:
        return {'r': 34, 'g': 139, 'b': 34, 'a': 240}
    else:
        return {'r': 0, 'g': 100, 'b': 0, 'a': 255}

# Specific fuel moisture color functions
def mc1_to_color(moisture_percent):
    return fuel_moisture_to_color(moisture_percent, 40)

def mc10_to_color(moisture_percent):
    return fuel_moisture_to_color(moisture_percent, 40)

def mc100_to_color(moisture_percent):
    return fuel_moisture_to_color(moisture_percent, 40)

def mc1000_to_color(moisture_percent):
    return fuel_moisture_to_color(moisture_percent, 40)

def mc_wood_to_color(moisture_percent):
    return fuel_moisture_to_color(moisture_percent, 200)

def mc_herb_to_color(moisture_percent):
    return fuel_moisture_to_color(moisture_percent, 250)

# Fire danger color functions
def fire_danger_to_color(value, max_value=100):
    ratio = min(value / max_value, 1)
    
    if ratio <= 0:
        return {'r': 0, 'g': 100, 'b': 0, 'a': 255}
    elif ratio < 0.2:
        return {'r': 50, 'g': 205, 'b': 50, 'a': 240}
    elif ratio < 0.4:
        return {'r': 255, 'g': 255, 'b': 0, 'a': 220}
    elif ratio < 0.6:
        return {'r': 255, 'g': 165, 'b': 0, 'a': 240}
    elif ratio < 0.8:
        return {'r': 255, 'g': 69, 'b': 0, 'a': 255}
    elif ratio < 0.9:
        return {'r': 220, 'g': 20, 'b': 60, 'a': 255}
    elif ratio < 1.0:
        return {'r': 139, 'g': 0, 'b': 0, 'a': 255}
    else:
        return {'r': 0, 'g': 0, 'b': 0, 'a': 255}

def ic_to_color(ic_value):
    return fire_danger_to_color(ic_value, 100)

def erc_to_color(erc_value):
    return fire_danger_to_color(erc_value, 100)

def bi_to_color(bi_value):
    return fire_danger_to_color(bi_value, 200)

def sc_to_color(sc_value):
    return fire_danger_to_color(sc_value, 100)

def kbdi_to_color(kbdi_value):
    if kbdi_value <= 0:
        return {'r': 34, 'g': 139, 'b': 34, 'a': 255}
    elif kbdi_value < 100:
        return {'r': 173, 'g': 255, 'b': 47, 'a': 240}
    elif kbdi_value < 200:
        return {'r': 255, 'g': 255, 'b': 0, 'a': 220}
    elif kbdi_value < 300:
        return {'r': 255, 'g': 165, 'b': 0, 'a': 240}
    elif kbdi_value < 400:
        return {'r': 255, 'g': 69, 'b': 0, 'a': 255}
    elif kbdi_value < 500:
        return {'r': 220, 'g': 20, 'b': 60, 'a': 255}
    elif kbdi_value < 600:
        return {'r': 139, 'g': 0, 'b': 0, 'a': 255}
    elif kbdi_value < 700:
        return {'r': 75, 'g': 0, 'b': 0, 'a': 255}
    else:
        return {'r': 0, 'g': 0, 'b': 0, 'a': 255}

def gsi_to_color(gsi_value):
    if gsi_value <= 0:
        return {'r': 139, 'g': 69, 'b': 19, 'a': 255}
    elif gsi_value < 0.2:
        return {'r': 205, 'g': 133, 'b': 63, 'a': 240}
    elif gsi_value < 0.4:
        return {'r': 245, 'g': 222, 'b': 179, 'a': 220}
    elif gsi_value < 0.6:
        return {'r': 255, 'g': 250, 'b': 205, 'a': 200}
    elif gsi_value < 0.8:
        return {'r': 173, 'g': 255, 'b': 47, 'a': 220}
    elif gsi_value < 0.9:
        return {'r': 50, 'g': 205, 'b': 50, 'a': 240}
    else:
        return {'r': 0, 'g': 100, 'b': 0, 'a': 255}

# Variable mapping
VARIABLE_COLOR_MAP = {
    'wspd': wind_speed_to_color,
    'tmp': temperature_to_color,
    'rh': humidity_to_color,
    'mc1': mc1_to_color,
    'mc10': mc10_to_color,
    'mc100': mc100_to_color,
    'mc1000': mc1000_to_color,
    'mcwood': mc_wood_to_color,
    'mcherb': mc_herb_to_color,
    'kbdi': kbdi_to_color,
    'ic': ic_to_color,
    'erc': erc_to_color,
    'bi': bi_to_color,
    'sc': sc_to_color,
    'gsi': gsi_to_color,
}