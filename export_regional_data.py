import argparse
import math
import duckdb
import pandas as pd

parser = argparse.ArgumentParser(description="导出区域内的运动数据")
parser.add_argument('--lat', type=float, required=True, help="中心点纬度")
parser.add_argument('--lon', type=float, required=True, help="中心点经度")
parser.add_argument('--distance', type=int, required=True, help="范围(米)")
args = parser.parse_args()

def parse_time(val):
    if val is None: return 0.0
    if isinstance(val, (int, float)): return float(val)
    val_str = str(val).strip()
    if ' ' in val_str: val_str = val_str.split(' ')[-1]
    try:
        parts = val_str.split(':')
        if len(parts) == 3: return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
        return float(val_str)
    except ValueError: return 0.0

def safe_float(val):
    if val is None: return 0.0
    try: return float(val)
    except ValueError: return 0.0

def decode_polyline(polyline_str):
    if not polyline_str: return []
    index, lat, lng = 0, 0, 0
    coordinates = []
    changes = {'latitude': 0, 'longitude': 0}
    while index < len(polyline_str):
        for unit in ['latitude', 'longitude']:
            shift, result = 0, 0
            while True:
                byte = ord(polyline_str[index]) - 63
                index += 1
                result |= (byte & 0x1f) << shift
                shift += 5
                if not byte >= 0x20: break
            if (result & 1): changes[unit] = ~(result >> 1)
            else: changes[unit] = (result >> 1)
        lat += changes['latitude']
        lng += changes['longitude']
        coordinates.append([lng / 100000.0, lat / 100000.0])
    return coordinates

def haversine(lon1, lat1, lon2, lat2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi, delta_lambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

print(f"🔍 正在检索以 [{args.lat}, {args.lon}] 为中心，{args.distance}米为半径的运动数据...")

sql = """
SELECT 
    summary_polyline, type, distance, moving_time, average_heartrate, elevation_gain 
FROM read_parquet('data.parquet') 
WHERE summary_polyline IS NOT NULL
"""

with duckdb.connect() as conn:
    try:
        raw_rows = conn.execute(sql).fetchall()
    except Exception as e:
        print(f"❌ 读取数据失败: {e}")
        exit(1)

filtered_data = []

for row in raw_rows:
    poly_str, m_type, dist_m, time_s, avg_hr, elev_g = row
    
    # 强制进行安全格式转换
    dist_m = safe_float(dist_m)
    time_s = parse_time(time_s)
    avg_hr = safe_float(avg_hr)
    elev_g = safe_float(elev_g)
    
    decoded_points = decode_polyline(poly_str)
    if not decoded_points or len(decoded_points) < 2: continue
        
    in_region = False
    for point in decoded_points:
        if haversine(point[0], point[1], args.lon, args.lat) <= args.distance:
            in_region = True
            break
            
    if in_region:
        filtered_data.append({
            '运动类型 (Type)': m_type,
            '距离-米 (Distance)': round(dist_m, 2),
            '运动时间-秒 (Time)': int(time_s),
            '平均心率 (Avg HR)': round(avg_hr, 1),
            '海拔爬升-米 (Elevation)': round(elev_g, 2)
        })

df = pd.DataFrame(filtered_data)
csv_filename = "regional_sports_data.csv"
df.to_csv(csv_filename, index=False, encoding='utf-8-sig')

print(f"✅ 成功提取 {len(filtered_data)} 条记录，已导出至 {csv_filename}")
