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

# --- 💥 核心修复：强大的时间与数字安全解析器 💥 ---
def parse_time(val):
    if val is None: return 0.0
    if isinstance(val, (int, float)): return float(val)
    val_str = str(val).strip()
    # 截取掉前面可能的日期 (例如 '1970-01-01 00:24:01' -> '00:24:01')
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
# ------------------------------------------------

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
            if (result & 1): changes[unit] = ~(result >> 1)
            else: changes[unit] = (result >> 1)
        lat += changes['latitude']
        lng += changes['longitude']
        coordinates.append([lng / 100000.0, lat / 100000.0])
    return coordinates

# --- 距离计算 ---
def haversine(lon1, lat1, lon2, lat2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi, delta_lambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
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
projector = MercatorProjector.from_bounds(poster_bounds, width_px, height_px)
project_func = getattr(projector, 'project', getattr(projector, 'lat_lon_to_pixel', getattr(projector, 'lon_lat_to_pixel', None)))

sql = """
SELECT 
    summary_polyline, type, distance, moving_time, average_heartrate, elevation_gain 
FROM read_parquet('data.parquet') 
WHERE summary_polyline IS NOT NULL
"""

with duckdb.connect() as conn:
    try:
        raw_rows = conn.execute(sql).fetchall()
        clean_rows = []
        # 使用安全解析器，强制转换为纯 Python float，彻底断绝运算崩溃可能
        for r in raw_rows:
            clean_rows.append((
                str(r[0]), str(r[1]),
                safe_float(r[2]),
                parse_time(r[3]),
                safe_float(r[4]),
                safe_float(r[5])
            ))
        raw_rows = clean_rows
    except Exception as e:
        print(f"⚠️ 读取统计数据失败 ({e})，部分数据可能显示为0。")
        fallback_sql = "SELECT summary_polyline, type FROM read_parquet('data.parquet') WHERE summary_polyline IS NOT NULL"
        fallback_rows = conn.execute(fallback_sql).fetchall()
        raw_rows = [(str(r[0]), str(r[1]), 0.0, 0.0, 0.0, 0.0) for r in fallback_rows]

print("步骤 3/3：注入矢量轨迹与统计面板...")

color_map = {
    'Run': '#FC4C02',       
    'Cycling': '#00DFD8',   
    'Ride': '#00DFD8',      
    'Hike': '#FFC300',      
    'Walk': '#A855F7',      
}
default_color = '#06D6A0'   
line_width = max(width_px * 0.0005, 0.75) 

run_count = ride_count = hike_count = walk_count = total_count = 0
run_dist_m = ride_dist_m = hike_dist_m = walk_dist_m = total_dist_m = 0
total_elev_g = total_weighted_hr = total_time_s = 0

run_routes = []
other_routes = []

for row in raw_rows:
    poly_str, m_type, dist_m, time_s, avg_hr, elev_g = row
    decoded_points = decode_polyline(poly_str)
    
    if not decoded_points or len(decoded_points) < 2: continue
        
    in_region = False
    for point in decoded_points:
        if haversine(point[0], point[1], args.lon, args.lat) <= args.distance:
            in_region = True
            break
            
    if not in_region: continue

    if m_type == 'Run':
        run_routes.append((decoded_points, m_type))
        run_count += 1; run_dist_m += dist_m
    else:
        other_routes.append((decoded_points, m_type))
        if m_type in ['Cycling', 'Ride']:
            ride_count += 1; ride_dist_m += dist_m
        elif m_type == 'Hike':
            hike_count += 1; hike_dist_m += dist_m
        elif m_type == 'Walk':
            walk_count += 1; walk_dist_m += dist_m
            
    total_count += 1
    total_dist_m += dist_m
    total_elev_g += elev_g
    total_weighted_hr += avg_hr * time_s
    total_time_s += time_s

run_dist_km = run_dist_m / 1000.0
ride_dist_km = ride_dist_m / 1000.0
hike_dist_km = hike_dist_m / 1000.0
walk_dist_km = walk_dist_m / 1000.0
total_dist_km = total_dist_m / 1000.0
total_avg_hr = total_weighted_hr / total_time_s if total_time_s > 0 else 0
total_time_h = int(total_time_s // 3600)
total_time_m = int((total_time_s % 3600) // 60)

run_text = f"{run_count} Runs {run_dist_km:.1f} km"
ride_text = f"{ride_count} Rides {ride_dist_km:.1f} km"
hike_text = f"{hike_count} Hikes {hike_dist_km:.1f} km"
walk_text = f"{walk_count} Walks {walk_dist_km:.1f} km"
hr_text = f"{int(total_avg_hr)} Avg Heart Rate"
elev_text = f"{int(total_elev_g)} m Elevation Gain"
total_text = f"Σ {total_count} Total {total_dist_km:.1f} km / {total_time_h} h {total_time_m} min"

svg_injection_lines = ['<g id="my_custom_tracks" fill="none" stroke-linecap="round" stroke-linejoin="round" opacity="0.95">']

def add_route_to_svg(lon_lat_list, m_type):
    pixel_points = []
    for point in lon_lat_list:
        lon, lat = point[0], point[1]
        x, y = project_func(lat, lon) if project_func.__name__ == 'lat_lon_to_pixel' else project_func(lon, lat)
        pixel_points.append(f"{x:.1f},{y:.1f}")
    color = color_map.get(m_type, default_color)
    pts_str = " ".join(pixel_points)
    svg_injection_lines.append(f'  <polyline points="{pts_str}" stroke="{color}" stroke-width="{line_width:.1f}" />')

for r, t in other_routes: add_route_to_svg(r, t)
for r, t in run_routes: add_route_to_svg(r, t)
svg_injection_lines.append('</g>')

with open(result.files[0], 'r', encoding='utf-8') as f:
    svg_content = f.read()

for block in re.findall(r'<text\b.*?</text>', svg_content, flags=re.IGNORECASE | re.DOTALL):
    if args.city not in block and args.province not in block:
        svg_content = svg_content.replace(block, '')

dark_glass = '<rect width="100%" height="100%" fill="#050505" opacity="0.5" />\n'
if '<text' in svg_content:
    svg_content = svg_content.replace('<text', dark_glass + '<text', 1)
else:
    svg_injection_lines.insert(0, dark_glass)

SHIFT_Y = 60         
TITLE_SCALE = 0.85   
SUBTITLE_SCALE = 1.4 

def add_translate(tag_str):
    if 'transform="' in tag_str:
        return re.sub(r'transform="([^"]+)"', rf'transform="\1 translate(0, {SHIFT_Y})"', tag_str)
    return tag_str.replace('/>', f' transform="translate(0, {SHIFT_Y})"/>', 1) if tag_str.endswith('/>') else tag_str.replace('>', f' transform="translate(0, {SHIFT_Y})">', 1)

def tweak_city(match):
    tag = match.group(0)
    tag = re.sub(r'font-size="([\d.]+)"', lambda m: f'font-size="{float(m.group(1)) * TITLE_SCALE:.1f}"', tag)
    return add_translate(tag)

svg_content = re.sub(rf'<text\b[^>]*>{args.city}</text>', tweak_city, svg_content)

prov_match = re.search(rf'<text\b[^>]*y="([\d.]+)"[^>]*>{args.province}</text>', svg_content)
stats_y_pos = (float(prov_match.group(1)) + SHIFT_Y + 120) if prov_match else (height_px - 280)

def tweak_prov(match):
    tag = match.group(0)
    tag = re.sub(r'font-size="([\d.]+)"', lambda m: f'font-size="{float(m.group(1)) * SUBTITLE_SCALE:.1f}"', tag)
    return add_translate(tag)

svg_content = re.sub(rf'<text\b[^>]*>{args.province}</text>', tweak_prov, svg_content)
svg_content = re.sub(r'<line\b[^>]*>', lambda m: add_translate(m.group(0)), svg_content)

sigma_icon = """<path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15.5h-2v-2h2v2zm0-4.5h-2v-2h2v2zm0-4.5h-2v-2h2v2zm0-4.5h-2v-2h2v2zm2-2.5h-4v-2h4v2zm2 2.5h-2v-2h2v2z" fill="#f0f0f0"/>"""
run_icon = """<path d="M12.5,21.5L10.5,19.5L10.5,14.5L12.5,12.5L14.5,14.5L14.5,19.5L12.5,21.5z M13,22.5L12,21.5L13,20.5L14,21.5L13,22.5z M12,11.5L10,9.5L10,4.5L12,2.5L14,4.5L14,9.5L12,11.5z M12.5,10.5L11.5,9.5L11.5,4.5L12.5,3.5L13.5,4.5L13.5,9.5L12.5,10.5z M16.5,13.5L14.5,11.5L14.5,6.5L16.5,4.5L18.5,6.5L18.5,11.5L16.5,13.5z M17,14.5L16,13.5L17,12.5L18,13.5L17,14.5z" fill="#FC4C02"/>"""
ride_icon = """<path d="M15.5 2.5a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5zM12.5 1.5a.5.5 0 01.5-.5h1.5a.5.5 0 010 1H13a.5.5 0 01-.5-.5zM19.5 4a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM18.5 7a.5.5 0 01-.5-.5v-1a.5.5 0 011 0v1a.5.5 0 01-.5.5zM16.5 11.5c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707zM17.5 13a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM11.5 17c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707zM10.5 18a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM8.5 19.5a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM6.5 20a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM4.5 19.5a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM2.5 18a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM1.5 17c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707z" fill="#00DFD8"/>"""
hike_icon = """<path d="M12 1.5a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5zM10.5 1.5a.5.5 0 01.5-.5h1.5a.5.5 0 010 1h-1.5a.5.5 0 01-.5-.5zM17.5 4c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343-.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707zM16.5 5a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM14.5 7a.5.5 0 01-.5-.5v-1a.5.5 0 011 0v1a.5.5 0 01-.5.5zM12.5 11.5c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343-.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707zM11.5 13a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5z" fill="#FFC300"/>"""
walk_icon = """<path d="M12.5 21.5l-2.001-2.001V14.5l2.001-2.001v-1l1 1-1-1 1-1L12.5 21.5zm.5 1l-1-1 1-1 1 1-1 1z" fill="#A855F7"/>"""
heart_icon = """<path d="M12.5 21.5a5.501 5.501 0 005.5-5.5 5.501 5.501 0 00-5.5-5.5 5.501 5.501 0 00-5.5 5.5 5.501 5.501 0 005.5 5.5z M13 18a.5.5 0 01-.5-.5V16a.5.5 0 011 0v1.5a.5.5 0 01-.5.5z" fill="#f0f0f0"/>"""
elev_icon = """<path d="M12.5 1.5a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5zM10.5 1.5a.5.5 0 01.5-.5h1.5a.5.5 0 010 1h-1.5a.5.5 0 01-.5-.5zM18.5 4c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343-.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707zM17.5 5a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5z" fill="#f0f0f0"/>"""

stats_block = (
    f'<g id="stats_block" transform="translate({width_px/2:.1f}, {stats_y_pos:.1f})" text-anchor="middle" fill="#f0f0f0" font-family="Arial, Helvetica, sans-serif">\n'
    f'  <g id="stats_items_grid" transform="translate(0, 0)" font-size="20">\n'
    f'    <g transform="translate(-180, 0)"><g transform="translate(-30, 0)"> {run_icon} </g><text transform="translate(30, 20)" text-anchor="start">{run_text}</text></g>\n'
    f'    <g transform="translate(180, 0)"><g transform="translate(-30, 0)"> {ride_icon} </g><text transform="translate(30, 20)" text-anchor="start">{ride_text}</text></g>\n'
    f'    <g transform="translate(-180, 50)"><g transform="translate(-30, 0)"> {hike_icon} </g><text transform="translate(30, 20)" text-anchor="start">{hike_text}</text></g>\n'
    f'    <g transform="translate(180, 50)"><g transform="translate(-30, 0)"> {walk_icon} </g><text transform="translate(30, 20)" text-anchor="start">{walk_text}</text></g>\n'
    f'    <g transform="translate(-120, 100)"><g transform="translate(-30, 0)"> {heart_icon} </g><text transform="translate(30, 20)" text-anchor="start">{hr_text}</text></g>\n'
    f'    <g transform="translate(120, 100)"><g transform="translate(-30, 0)"> {elev_icon} </g><text transform="translate(30, 20)" text-anchor="start">{elev_text}</text></g>\n'
    f'  </g>\n'
    f'  <g id="total_summary" transform="translate(0, 160)" font-size="24" font-weight="bold">\n'
    f'    <g transform="translate(-160, 0)"><g transform="translate(-30, 0)"> {sigma_icon} </g><text transform="translate(30, 20)" text-anchor="start">{total_text}</text></g>\n'
    f'  </g>\n'
    f'</g>\n'
)

if "</svg>" in svg_content:
    svg_content = svg_content.replace("</svg>", "\n".join(svg_injection_lines) + "\n" + stats_block + "\n</svg>")

final_path = "colorful-map.svg"
with open(final_path, 'w', encoding='utf-8') as f:
    f.write(svg_content)

print(f"\n大功告成！海报已生成：{final_path}")
