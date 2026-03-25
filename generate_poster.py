import argparse
from pathlib import Path
import duckdb
import re
import math
from terraink_py import PosterRequest, generate_poster
from terraink_py.api import MercatorProjector

# --- 接收 GitHub Actions 传来的参数 ---
parser = argparse.ArgumentParser(description="生成运动轨迹海报")
parser.add_argument('--lat', type=float, required=True, help="中心点纬度")
parser.add_argument('--lon', type=float, required=True, help="中心点经度")
parser.add_argument('--distance', type=int, required=True, help="范围(米)")
parser.add_argument('--city', type=str, required=True, help="城市")
parser.add_argument('--province', type=str, required=True, help="省份")
args = parser.parse_args()

# --- Polyline 解密 ---
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
                if not byte >= 0x20:
                    break
            if (result & 1):
                changes[unit] = ~(result >> 1)
            else:
                changes[unit] = (result >> 1)
        lat += changes['latitude']
        lng += changes['longitude']
        coordinates.append([lng / 100000.0, lat / 100000.0])
    return coordinates

# --- 距离计算 ---
def haversine(lon1, lat1, lon2, lat2):
    R = 6371000
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

print(f"步骤 1/3：正在生成 {args.distance}m 范围的基础地图...")

result = generate_poster(
    PosterRequest(
        output=Path("./base-map"),
        formats=("svg",), 
        lat=args.lat,  
        lon=args.lon, 
        title=args.city,
        subtitle=args.province,
        theme="dark",   
        width_cm=21,
        height_cm=33, 
        distance_m=args.distance, 
        include_buildings=True,
    )
)

print("步骤 2/3：读取并汇总云端运动数据...")

poster_bounds = result.bounds.poster_bounds
width_px = result.size.width
height_px = result.size.height
projector = Merc
