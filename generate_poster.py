from pathlib import Path
import duckdb
import re
import math
from terraink_py import PosterRequest, generate_poster
from terraink_py.api import MercatorProjector

# --- 🚀 Polyline 轨迹解密函数 ---
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
# --------------------------------------------------

# --- 📏 大圆距离计算函数 (Haversine Formula) ---
def haversine(lon1, lat1, lon2, lat2):
    """
    计算两点经纬度之间的大圆距离 (单位：米)
    """
    R = 6371000  # 地球半径 (米)
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c
# --------------------------------------------------

print("步骤 1/3：正在生成 18km 范围的 SVG 基础地图...")

# 1. 设置中心点和半径 (用于地图生成和数据汇总)
center_lat = 35.30500318
center_lon = 113.93085256
radius_m = 18000

# 2. 生成干净的 SVG 底图 (去掉了报错的 theme_data，回归最稳妥的 dark)
result = generate_poster(
    PosterRequest(
        output=Path("outputs/xinxiang-extreme-dark-base"),
        formats=("svg",), 
        lat=center_lat,  
        lon=center_lon, 
        title="新乡市",
        subtitle="河南省",
        theme="dark",   # 💥 回归默认的 dark，后面我们自己加滤镜变暗！
        width_cm=21,
        height_cm=29.7,
        distance_m=radius_m, 
        include_buildings=True,
    )
)

print("步骤 2/3：读取并解密运动数据，并按区域过滤汇总统计数据...")

poster_bounds = result.bounds.poster_bounds
width_px = result.size.width
height_px = result.size.height
projector = MercatorProjector.from_bounds(poster_bounds, width_px, height_px)
project_func = getattr(projector, 'project', getattr(projector, 'lat_lon_to_pixel', getattr(projector, 'lon_lat_to_pixel', None)))

# 3. 读取 Parquet 文件 (假设 Parquet 文件是在本地通过脚本从 .db 文件转换的，其列名取决于 Pandas 转换时的模式假设，我这里假设标准列名)
# Parquet 模式假设： summary_polyline (encoded polylines), type (sport type), distance_meters (total distance), moving_time_seconds (total moving time), average_heartrate (avg HR), elevation_gain_meters (total elevation gain).
# 如果Parquet不包含这些列，脚本将无法正常工作。

parquet_path = r"C:\Users\yaosh\Desktop\data.parquet"
# 假设 Parquet 存在

sql = f"SELECT summary_polyline, type, distance_meters, moving_time_seconds, average_heartrate, elevation_gain_meters FROM read_parquet('{parquet_path}') WHERE summary_polyline IS NOT NULL"

with duckdb.connect() as conn:
    try:
        raw_rows = conn.execute(sql).fetchall()
    except duckdb.BinderException:
        fallback_sql = f"SELECT summary_polyline, type FROM read_parquet('{parquet_path}') WHERE summary_polyline IS NOT NULL"
        print("💥 警告：Parquet 文件中没有找到统计数据列。将只显示计数，不显示距离/心率/海拔增益。")
        raw_rows = [(r[0], r[1], 0, 0, 0, 0) for r in conn.execute(fallback_sql).fetchall()] # 如果Parquet不包含这些列，脚本将无法正常工作。

print("步骤 3/3：注入矢量轨迹...")

# ==========================================
# 🎨 Strava 风格高级荧光配色方案
# ==========================================
color_map = {
    'Run': '#FC4C02',       # Strava 经典橙红
    'Cycling': '#00DFD8',   # 电光青蓝
    'Ride': '#00DFD8',      # 电光青蓝
    'Hike': '#FFC300',      # 琥珀明黄
    'Walk': '#A855F7',      # 霓虹紫
}
default_color = '#06D6A0'   # 薄荷绿
line_width = max(width_px * 0.0005, 0.75) 

# --- 按区域过滤和汇总统计数据变量定义 ---
# 汇总变量，修正汇总逻辑以按运动类型汇总。我汇总区域内的所有活动的总距离，时间，海拔，心率，如果不包含其他运动，则跳过。
run_count = 0; run_dist_m = 0; run_elev_g = 0; run_weighted_hr = 0; run_time_s = 0
ride_count = 0; ride_dist_m = 0; ride_elev_g = 0; ride_weighted_hr = 0; ride_time_s = 0
hike_count = 0; hike_dist_m = 0; hike_elev_g = 0; hike_weighted_hr = 0; hike_time_s = 0
walk_count = 0; walk_dist_m = 0; walk_elev_g = 0; walk_weighted_hr = 0; walk_time_s = 0
total_count = 0; total_dist_m = 0; total_elev_g = 0; total_weighted_hr = 0; total_time_s = 0

run_routes = []
other_routes = []

for row in raw_rows:
    poly_str, m_type, dist_m, time_s, avg_hr, elev_g = row
    decoded_points = decode_polyline(poly_str)
    
    if not decoded_points or len(decoded_points) < 2:
        continue
        
    # 1. 地理过滤：检查折线是否在圆内 (至少一点)
    in_region = False
    for point in decoded_points:
        dist = haversine(point[0], point[1], center_lon, center_lat)
        if dist <= radius_m:
            in_region = True
            break
            
    if not in_region:
        continue

    # 2. 将轨迹分类和注入
    if m_type == 'Run':
        run_routes.append((decoded_points, m_type))
        # 3. 汇总统计数据
        run_count += 1
        run_dist_m += dist_m
        run_elev_g += elev_g
        run_weighted_hr += avg_hr * time_s
        run_time_s += time_s
        total_count += 1; total_dist_m += dist_m; total_elev_g += elev_g; total_weighted_hr += avg_hr * time_s; total_time_s += time_s
    else:
        other_routes.append((decoded_points, m_type))
        if m_type == 'Cycling' or m_type == 'Ride':
            ride_count += 1
            ride_dist_m += dist_m
            ride_elev_g += elev_g
            ride_weighted_hr += avg_hr * time_s
            ride_time_s += time_s
            total_count += 1; total_dist_m += dist_m; total_elev_g += elev_g; total_weighted_hr += avg_hr * time_s; total_time_s += time_s
        elif m_type == 'Hike':
            hike_count += 1
            hike_dist_m += dist_m
            hike_elev_g += elev_g
            hike_weighted_hr += avg_hr * time_s
            hike_time_s += time_s
            total_count += 1; total_dist_m += dist_m; total_elev_g += elev_g; total_weighted_hr += avg_hr * time_s; total_time_s += time_s
        elif m_type == 'Walk':
            walk_count += 1
            walk_dist_m += dist_m
            walk_elev_g += elev_g
            walk_weighted_hr += avg_hr * time_s
            walk_time_s += time_s
            total_count += 1; total_dist_m += dist_m; total_elev_g += elev_g; total_weighted_hr += avg_hr * time_s; total_time_s += time_s
        else:
            # 其他运动不包括在特定的计数中，但可以包括在总计中
            total_count += 1; total_dist_m += dist_m; total_elev_g += elev_g; total_weighted_hr += avg_hr * time_s; total_time_s += time_s

# --- 格式化统计数据文本 ---
# 我按运动类型汇总了距离。

run_dist_km = run_dist_m / 1000.0
ride_dist_km = ride_dist_m / 1000.0
hike_dist_km = hike_dist_m / 1000.0
walk_dist_km = walk_dist_m / 1000.0
total_dist_km = total_dist_m / 1000.0
total_avg_hr = total_weighted_hr / total_time_s if total_time_s > 0 else 0
total_elev_m = total_elev_g
total_time_h = total_time_s // 3600
total_time_m = (total_time_s % 3600) // 60

# 格式化文本
run_text = f"{run_count} Runs {run_dist_km:.1f} km"
ride_text = f"{ride_count} Rides {ride_dist_km:.1f} km"
hike_text = f"{hike_count} Hikes {hike_dist_km:.1f} km"
walk_text = f"{walk_count} Walks {walk_dist_km:.1f} km"
hr_text = f"{int(total_avg_hr)} Avg Heart Rate"
elev_text = f"{int(total_elev_m)} m Elevation Gain"
total_text = f"Σ {total_count} Total {total_dist_km:.1f} km / {total_time_h} h {total_time_m} min"
# --- ---------------------- ---

# 提高不透明度，让高饱和度颜色燃起来
svg_injection_lines = [
    '<g id="my_custom_tracks" fill="none" stroke-linecap="round" stroke-linejoin="round" opacity="0.95">'
]

def add_route_to_svg(lon_lat_list, m_type):
    pixel_points = []
    for point in lon_lat_list:
        lon, lat = point[0], point[1]
        if project_func.__name__ == 'lat_lon_to_pixel':
            x, y = project_func(lat, lon)
        else:
            x, y = project_func(lon, lat)
        pixel_points.append(f"{x:.1f},{y:.1f}")
    color = color_map.get(m_type, default_color)
    pts_str = " ".join(pixel_points)
    svg_injection_lines.append(f'  <polyline points="{pts_str}" stroke="{color}" stroke-width="{line_width:.1f}" />')

# 先画其他，最后画跑步（置于顶层）
for r, t in other_routes:
    add_route_to_svg(r, t)
for r, t in run_routes:
    add_route_to_svg(r, t)
svg_injection_lines.append('</g>')

base_svg_path = result.files[0]
with open(base_svg_path, 'r', encoding='utf-8') as f:
    svg_content = f.read()

# ==========================================
# 🎨 终极微调区：去水印 + 暗色滤镜 + 排版
# ==========================================

# 1. 抹除水印文字
text_blocks = re.findall(r'<text\b.*?</text>', svg_content, flags=re.IGNORECASE | re.DOTALL)
for block in text_blocks:
    if '新乡' not in block and '河南' not in block:
        svg_content = svg_content.replace(block, '')

# 💥 2. 神级操作：添加“暗色玻璃滤镜” 💥
# 我们在文字（新乡市）之前，强行铺上一层半透明的黑色，把底层的道路和建筑强行压暗！
dark_glass = '<rect width="100%" height="100%" fill="#050505" opacity="0.5" />\n'
if '<text' in svg_content:
    svg_content = svg_content.replace('<text', dark_glass + '<text', 1)
else:
    svg_injection_lines.insert(0, dark_glass)

# 3. 排版微调 (保持不变)
# 这里使用 SHIFT_Y=60. 在脚本中是 SHIFT_Y=60.
SHIFT_Y = 60         
TITLE_SCALE = 0.85   
SUBTITLE_SCALE = 1.4 

def add_translate(tag_str):
    if 'transform="' in tag_str:
        return re.sub(r'transform="([^"]+)"', rf'transform="\1 translate(0, {SHIFT_Y})"', tag_str)
    else:
        if tag_str.endswith('/>'):
            return tag_str.replace('/>', f' transform="translate(0, {SHIFT_Y})"/>', 1)
        else:
            return tag_str.replace('>', f' transform="translate(0, {SHIFT_Y})">', 1)

svg_content = re.sub(r'<text\b[^>]*>新乡市</text>', lambda m: add_translate(re.sub(r'font-size="([\d.]+)"', lambda m2: f'font-size="{float(m2.group(1)) * TITLE_SCALE:.1f}"', m.group(0))), svg_content)
# 我将寻找原始 y，计算 stats_y。
# 寻找原始 y 坐标。 `Henan_match = re.search(r'<text\b[^>]*y="([\d.]+)"[^>]*>河南省</text>', svg_content)` 找到原始 y。 `shift_y = 60`。 `stats_y = original_Henan_y + shift_y + 120`。
# 寻找原始 y 坐标
henan_match = re.search(r'<text\b[^>]*y="([\d.]+)"[^>]*>河南省</text>', svg_content)
original_henan_y = float(henan_match.group(1))
new_henan_y = original_henan_y + SHIFT_Y
svg_content = re.sub(r'<text\b[^>]*>河南省</text>', lambda m: add_translate(re.sub(r'font-size="([\d.]+)"', lambda m2: f'font-size="{float(m2.group(1)) * SUBTITLE_SCALE:.1f}"', m.group(0))), svg_content)
svg_content = re.sub(r'<line\b[^>]*>', lambda m: add_translate(m.group(0)), svg_content)

# ==========================================
# 💥 插入按地理汇总的运动统计数据块 💥
# ==========================================
# 确定统计块 y 位置：在河南省文本下移后的 y 坐标下方。
stats_y_pos = new_henan_y + 120 # 在河南省文本下移后的 y 坐标下方。垂直间距 120。

# 图标路径数据 (假设标准开源图标路径数据)
# Σ (Sigma) 图标
sigma_icon = '<path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 15.5h-2v-2h2v2zm0-4.5h-2v-2h2v2zm0-4.5h-2v-2h2v2zm0-4.5h-2v-2h2v2zm2-2.5h-4v-2h4v2zm2 2.5h-2v-2h2v2z" fill="#f0f0f0"/>'
# 跑步图标 (running man)
run_icon_path = '<path d="M12.5,21.5L10.5,19.5L10.5,14.5L12.5,12.5L14.5,14.5L14.5,19.5L12.5,21.5z M13,22.5L12,21.5L13,20.5L14,21.5L13,22.5z M12,11.5L10,9.5L10,4.5L12,2.5L14,4.5L14,9.5L12,11.5z M12.5,10.5L11.5,9.5L11.5,4.5L12.5,3.5L13.5,4.5L13.5,9.5L12.5,10.5z M16.5,13.5L14.5,11.5L14.5,6.5L16.5,4.5L18.5,6.5L18.5,11.5L16.5,13.5z M17,14.5L16,13.5L17,12.5L18,13.5L17,14.5z" fill="#FC4C02"/>'
# 骑行图标 (bicycle)
ride_icon_path = '<path d="M15.5 2.5a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5zM12.5 1.5a.5.5 0 01.5-.5h1.5a.5.5 0 010 1H13a.5.5 0 01-.5-.5zM19.5 4a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM18.5 7a.5.5 0 01-.5-.5v-1a.5.5 0 011 0v1a.5.5 0 01-.5.5zM16.5 11.5c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343-.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707zM17.5 13a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM11.5 17c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343-.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707zM10.5 18a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM8.5 19.5a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM6.5 20a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM4.5 19.5a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM2.5 18a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM1.5 17c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343-.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707z" fill="#00DFD8"/>'
# 徒步图标 (hiking boot)
hike_icon_path = '<path d="M12 1.5a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5zM10.5 1.5a.5.5 0 01.5-.5h1.5a.5.5 0 010 1h-1.5a.5.5 0 01-.5-.5zM17.5 4c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343-.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707zM16.5 5a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5zM14.5 7a.5.5 0 01-.5-.5v-1a.5.5 0 011 0v1a.5.5 0 01-.5.5zM12.5 11.5c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343-.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707zM11.5 13a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5z" fill="#FFC300"/>'
# 步行图标 (walking man)
walk_icon_path = '<path d="M12.5 21.5l-2.001-2.001V14.5l2.001-2.001v-1l1 1-1-1 1-1L12.5 21.5zm.5 1l-1-1 1-1 1 1-1 1z" fill="#A855F7"/>'
# 心率图标 (heart)
heart_icon_path = '<path d="M12.5 21.5a5.501 5.501 0 005.5-5.5 5.501 5.501 0 00-5.5-5.5 5.501 5.501 0 00-5.5 5.5 5.501 5.501 0 005.5 5.5z M13 18a.5.5 0 01-.5-.5V16a.5.5 0 011 0v1.5a.5.5 0 01-.5.5z" fill="#f0f0f0"/>'
# 海拔增益图标 (mountain)
elev_icon_path = '<path d="M12.5 1.5a.5.5 0 01.5-.5h2a.5.5 0 010 1h-2a.5.5 0 01-.5-.5zM10.5 1.5a.5.5 0 01.5-.5h1.5a.5.5 0 010 1h-1.5a.5.5 0 01-.5-.5zM18.5 4c.343.343.343.899 0 1.242a.5.5 0 010-.707c.343-.343.343-.899 0-1.242a.5.5 0 01-.707.707c.343.343.343.899 0 1.242a.5.5 0 01.707-.707zM17.5 5a.5.5 0 01-.5-.5v-.5a.5.5 0 011 0v.5a.5.5 0 01-.5.5z" fill="#f0f0f0"/>'

# 构建统计块 SVG 代码 (基于用户提供的图片布局 Assumptions (两列布局，居中))
stats_block = f"""
    <g id="stats_block" transform="translate({width_px/2}, {stats_y_pos})" text-anchor="middle" fill="#f0f0f0" font-family="Arial, Helvetica, sans-serif">
        <g id="stats_items_grid" transform="translate(0, 0)" font-size="20">
            <g transform="translate(-180, 0)">
                <g transform="translate(-30, 0)"> {run_icon_path} </g>
                <text transform="translate(30, 20)" text-anchor="start">{run_text}</text>
            </g>
            <g transform="translate(180, 0)">
                <g transform="translate(-30, 0)"> {ride_icon_path} </g>
                <text transform="translate(30, 20)" text-anchor="start">{ride_text}</text>
            </g>
            <g transform="translate(-180, 50)">
                <g transform="translate(-30, 0)"> {hike_icon_path} </g>
                <text transform="translate(30, 20)" text-anchor="start">{hike_text}</text>
            </g>
            <g transform="translate(180, 50)">
                <g transform="translate(-30, 0)"> {walk_icon_path} </g>
                <text transform="translate(30, 20)" text-anchor="start">{walk_text}</text>
            </g>
            <g transform="translate(-120, 100)">
                <g transform="translate(-30, 0)"> {heart_icon_path} </g>
                <text transform="translate(30, 20)" text-anchor="start">{hr_text}</text>
            </g>
            <g transform="translate(120, 100)">
                <g transform="translate(-30, 0)"> {elev_icon_path} </g>
                <text transform="translate(30, 20)" text-anchor="start">{elev_text}</text>
            </g>
        </g>
        <g id="total_summary" transform="translate(0, 160)" font-size="24" font-weight="bold">
            <g transform="translate(-160, 0)">
                <g transform="translate(-30, 0)"> {sigma_icon} </g>
                <text transform="translate(30, 20)" text-anchor="start">{total_text}</text>
            </g>
        </g>
    </g>
"""

# 插入到 SVG 结尾标签之前。就像轨迹层一样。
if "</svg>" in svg_content:
    svg_content = svg_content.replace("</svg>", stats_block + "\n</svg>")
# ==========================================

# 插入我们的自定义轨迹层 (置于最顶层，除了统计块)
if "</svg>" in svg_content:
    svg_content = svg_content.replace("</svg>", "\n".join(svg_injection_lines) + "\n</svg>")

final_path = r"C:\Users\yaosh\Desktop\xinxiang-colorful-map.svg"
with open(final_path, 'w', encoding='utf-8') as f:
    f.write(svg_content)

print(f"\n大功告成！海报已保存到桌面：")
print(f"{final_path}")
