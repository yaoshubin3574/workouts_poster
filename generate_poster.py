import argparse
import base64
import concurrent.futures
import io
import math
import re
import urllib.request
from pathlib import Path

import duckdb
import numpy as np
from PIL import Image
from terraink_py import PosterRequest, generate_poster
from terraink_py.api import MercatorProjector

parser = argparse.ArgumentParser(description="生成运动轨迹海报")
parser.add_argument("--lat", type=float, required=True, help="中心点纬度")
parser.add_argument("--lon", type=float, required=True, help="中心点经度")
parser.add_argument("--distance", type=int, required=True, help="范围(米)")
parser.add_argument("--city", type=str, required=True, help="城市")
args = parser.parse_args()


# ==========================================
# 🏔️ 核心算法：真实 3D 山影图 (DEM Hillshade) 生成器
# ==========================================
def generate_hillshade_image(bounds, width_px, height_px, distance_m, lat, lon):
    """
    根据海报的 Mercator 边界，从全球 DEM 瓦片计算 3D 浮雕山影图
    """
    if distance_m <= 8000:
        zoom = 12
    elif distance_m <= 25000:
        zoom = 11
    elif distance_m <= 65000:
        zoom = 10
    else:
        zoom = 9

    def lat_lon_to_tile(l_lat, l_lon, z):
        n = 2.0**z
        x = int((l_lon + 180.0) / 360.0 * n)
        lat_rad = math.radians(l_lat)
        y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return x, y

    def to_norm_xy(l_lat, l_lon):
        x = (l_lon + 180.0) / 360.0
        lat_rad = math.radians(l_lat)
        y = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0
        return x, y

    # 预留外围缓冲，确保梯度平滑
    pad_lat = (bounds.north - bounds.south) * 0.05
    pad_lon = (bounds.east - bounds.west) * 0.05
    tx_min, ty_min = lat_lon_to_tile(
        bounds.north + pad_lat, bounds.west - pad_lon, zoom
    )
    tx_max, ty_max = lat_lon_to_tile(
        bounds.south - pad_lat, bounds.east + pad_lon, zoom
    )

    max_t = (2**zoom) - 1
    tx_min, tx_max = max(0, tx_min), min(max_t, tx_max)
    ty_min, ty_max = max(0, ty_min), min(max_t, ty_max)

    def fetch_tile(coords):
        tx, ty = coords
        url = f"https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{zoom}/{tx}/{ty}.png"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=6) as resp:
                return (tx, ty, Image.open(io.BytesIO(resp.read())).convert("RGB"))
        except Exception:
            return (tx, ty, Image.new("RGB", (256, 256), (128, 0, 0)))

    tiles_coords = [
        (x, y) for y in range(ty_min, ty_max + 1) for x in range(tx_min, tx_max + 1)
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        fetched = list(executor.map(fetch_tile, tiles_coords))

    tile_dict = {(x, y): img for x, y, img in fetched}
    w_tiles = (tx_max - tx_min + 1) * 256
    h_tiles = (ty_max - ty_min + 1) * 256
    stitched = Image.new("RGB", (w_tiles, h_tiles))
    for (tx, ty), img in tile_dict.items():
        stitched.paste(img, ((tx - tx_min) * 256, (ty - ty_min) * 256))

    arr = np.array(stitched, dtype=np.float32)
    # 解码 Terrarium 高程数据 (米)
    elev = (arr[:, :, 0] * 256.0 + arr[:, :, 1] + arr[:, :, 2] / 256.0) - 32768.0

    res = (40075016.0 * math.cos(math.radians(lat))) / (256.0 * (2**zoom))
    dy, dx = np.gradient(elev, res, res)
    slope = np.arctan(np.sqrt(dx * dx + dy * dy))
    aspect = np.arctan2(-dy, dx)

    # 315° 方位角，45° 仰角太阳光照
    az_rad = np.radians(360.0 - 315.0 + 90.0)
    alt_rad = np.radians(45.0)
    shaded = np.sin(alt_rad) * np.cos(slope) + np.cos(alt_rad) * np.sin(slope) * np.cos(
        az_rad - aspect
    )
    shaded = np.clip(shaded, 0.0, 1.0)

    # 💥 关键暗黑遮罩：平原区域透明度为 0，只有起伏山体呈现微光浮雕
    slope_mask = 1.0 - np.exp(-slope * 6.5)
    alpha = (slope_mask * 210).astype(np.uint8)

    # 山体光影：暗色岩石高光（~110）与暗黑阴影（~15）
    val = (shaded * 110 + 15).astype(np.uint8)
    rgba = np.stack(
        [
            val,
            (val * 1.05).clip(0, 255).astype(np.uint8),
            (val * 1.12).clip(0, 255).astype(np.uint8),
            alpha,
        ],
        axis=-1,
    )
    full_hill = Image.fromarray(rgba, "RGBA")

    # 精准裁剪至当前海报画布
    nw_x, nw_y = to_norm_xy(bounds.north, bounds.west)
    se_x, se_y = to_norm_xy(bounds.south, bounds.east)

    px0 = (nw_x * (2**zoom) - tx_min) * 256.0
    py0 = (nw_y * (2**zoom) - ty_min) * 256.0
    px1 = (se_x * (2**zoom) - tx_min) * 256.0
    py1 = (se_y * (2**zoom) - ty_min) * 256.0

    cropped = full_hill.crop((px0, py0, px1, py1))
    return cropped.resize((width_px, height_px), Image.Resampling.BILINEAR)


def parse_time(val):
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    val_str = str(val).strip()
    if " " in val_str:
        val_str = val_str.split(" ")[-1]
    try:
        parts = val_str.split(":")
        if len(parts) == 3:
            return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2:
            return float(parts[0]) * 60 + float(parts[1])
        return float(val_str)
    except ValueError:
        return 0.0


def safe_float(val):
    if val is None:
        return 0.0
    try:
        return float(val)
    except ValueError:
        return 0.0


def decode_polyline(polyline_str):
    if not polyline_str:
        return []
    index, lat, lng = 0, 0, 0
    coordinates = []
    changes = {"latitude": 0, "longitude": 0}
    while index < len(polyline_str):
        for unit in ["latitude", "longitude"]:
            shift, result = 0, 0
            while True:
                byte = ord(polyline_str[index]) - 63
                index += 1
                result |= (byte & 0x1F) << shift
                shift += 5
                if not byte >= 0x20:
                    break
            if result & 1:
                changes[unit] = ~(result >> 1)
            else:
                changes[unit] = result >> 1
        lat += changes["latitude"]
        lng += changes["longitude"]
        coordinates.append([lng / 100000.0, lat / 100000.0])
    return coordinates


def haversine(lon1, lat1, lon2, lat2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi, delta_lambda = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


print(f"步骤 1/4：正在生成 {args.distance}m 范围的基础矢量地图...")

result = generate_poster(
    PosterRequest(
        output=Path("./base-map"),
        formats=("svg",),
        lat=args.lat,
        lon=args.lon,
        title=args.city,
        subtitle="",
        theme="dark",
        width_cm=21,
        height_cm=29.7,
        distance_m=args.distance,
        include_buildings=True,
    )
)

print("步骤 2/4：读取并汇总运动数据...")

poster_bounds = result.bounds.poster_bounds
width_px = result.size.width
height_px = result.size.height
projector = MercatorProjector.from_bounds(poster_bounds, width_px, height_px)
project_func = getattr(
    projector,
    "project",
    getattr(
        projector, "lat_lon_to_pixel", getattr(projector, "lon_lat_to_pixel", None)
    ),
)

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
        for r in raw_rows:
            clean_rows.append(
                (
                    str(r[0]),
                    str(r[1]),
                    safe_float(r[2]),
                    parse_time(r[3]),
                    safe_float(r[4]),
                    safe_float(r[5]),
                )
            )
        raw_rows = clean_rows
    except Exception as e:
        print(f"⚠️ 读取统计数据失败 ({e})，部分数据可能显示为0。")
        fallback_sql = "SELECT summary_polyline, type FROM read_parquet('data.parquet') WHERE summary_polyline IS NOT NULL"
        fallback_rows = conn.execute(fallback_sql).fetchall()
        raw_rows = [(str(r[0]), str(r[1]), 0.0, 0.0, 0.0, 0.0) for r in fallback_rows]

print("步骤 3/4：正在计算 3D 山影地貌 (DEM Hillshade)...")
hillshade_svg_tag = ""
has_hillshade = False

try:
    hill_img = generate_hillshade_image(
        poster_bounds, width_px, height_px, args.distance, args.lat, args.lon
    )
    buf = io.BytesIO()
    hill_img.save(buf, format="PNG")
    hill_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
    hillshade_svg_tag = (
        f'<image id="hillshade_relief" href="data:image/png;base64,{hill_b64}" '
        f'x="0" y="0" width="{width_px}" height="{height_px}" preserveAspectRatio="none" opacity="0.9" />'
    )
    hill_img.save("hillshade.png")
    has_hillshade = True
    print("✅ 成功生成 3D 山影浮雕图层！")
except Exception as e:
    print(f"⚠️ 山影图获取失败 ({e})，将平滑回退至纯矢量山体模式。")

print("步骤 4/4：注入高光轨迹、地貌与画廊排版...")

color_map = {
    "Run": "#FC4C02",
    "Cycling": "#22C55E",
    "Ride": "#22C55E",
    "Hike": "#FFC300",
    "Walk": "#A855F7",
}
default_color = "#06D6A0"
track_width = max(width_px * 0.0026, 5.5)

run_count = ride_count = hike_count = total_count = 0
run_dist_km = ride_dist_km = hike_dist_km = total_dist_km = 0
total_elev_g = total_weighted_hr = total_time_s = 0

run_routes, other_routes = [], []

for row in raw_rows:
    poly_str, m_type, dist_m, time_s, avg_hr, elev_g = row
    decoded_points = decode_polyline(poly_str)
    if not decoded_points or len(decoded_points) < 2:
        continue

    in_region = False
    for point in decoded_points:
        if haversine(point[0], point[1], args.lon, args.lat) <= args.distance:
            in_region = True
            break

    if not in_region:
        continue

    if m_type == "Run":
        run_routes.append((decoded_points, m_type))
        run_count += 1
        run_dist_km += dist_m / 1000.0
    else:
        other_routes.append((decoded_points, m_type))
        if m_type in ["Cycling", "Ride"]:
            ride_count += 1
            ride_dist_km += dist_m / 1000.0
        elif m_type == "Hike":
            hike_count += 1
            hike_dist_km += dist_m / 1000.0

    total_count += 1
    total_dist_km += dist_m / 1000.0
    total_elev_g += elev_g
    total_weighted_hr += avg_hr * time_s
    total_time_s += time_s

total_avg_hr = total_weighted_hr / total_time_s if total_time_s > 0 else 0
total_time_h = int(total_time_s // 3600)
total_time_m = int((total_time_s % 3600) // 60)

svg_injection_lines = [
    '<g id="my_custom_tracks" fill="none" stroke-linecap="round" stroke-linejoin="round">'
]


def add_route_to_svg(lon_lat_list, m_type):
    if not lon_lat_list or len(lon_lat_list) < 2:
        return
    pixel_points = []
    for point in lon_lat_list:
        lon, lat = point[0], point[1]
        x, y = (
            project_func(lat, lon)
            if project_func.__name__ == "lat_lon_to_pixel"
            else project_func(lon, lat)
        )
        pixel_points.append(f"{x:.1f},{y:.1f}")

    color = color_map.get(m_type, default_color)
    pts_str = " ".join(pixel_points)

    # 底层深黑描边（与道路/山影隔开）
    svg_injection_lines.append(
        f'  <polyline points="{pts_str}" stroke="#000000" stroke-width="{track_width * 1.6:.1f}" stroke-opacity="0.85" />'
    )
    # 中层霓虹外发光
    svg_injection_lines.append(
        f'  <polyline points="{pts_str}" stroke="{color}" stroke-width="{track_width * 2.6:.1f}" stroke-opacity="0.35" />'
    )
    # 顶层核心亮线
    svg_injection_lines.append(
        f'  <polyline points="{pts_str}" stroke="{color}" stroke-width="{track_width:.1f}" stroke-opacity="1.0" />'
    )

    # 起终点标记点
    s_x, s_y = pixel_points[0].split(",")
    e_x, e_y = pixel_points[-1].split(",")
    svg_injection_lines.append(
        f'  <circle cx="{s_x}" cy="{s_y}" r="{track_width * 1.5:.1f}" fill="#00E676" stroke="#FFFFFF" stroke-width="{track_width * 0.4:.1f}" />'
    )
    svg_injection_lines.append(
        f'  <circle cx="{e_x}" cy="{e_y}" r="{track_width * 1.5:.1f}" fill="#FF1744" stroke="#FFFFFF" stroke-width="{track_width * 0.4:.1f}" />'
    )


for r, t in other_routes:
    add_route_to_svg(r, t)
for r, t in run_routes:
    add_route_to_svg(r, t)
svg_injection_lines.append("</g>")

with open(result.files[0], "r", encoding="utf-8") as f:
    svg_content = f.read()

# ==========================================
# 💥 1. 精细化图层着色
# ==========================================
# 如果成功获得了真实 3D 山影，让生硬的平面色块变透明；否则使用沉稳墨绿作为兜底
park_color = "none" if has_hillshade else "#0e1813"

THEME_COLOR_MAP = {
    "#0a1628": "#000000",  # 陆地底色 -> 纯黑
    "#061020": "#152b42",  # 水系（西湖/黄河/水库）-> 深邃水体蓝
    "#0f2235": park_color,  # 山体 -> 真实山影时设为 none，让 3D 浮雕完全透出来
    "#6e5a45": "#181a1d",  # 建筑面要素 -> 极暗微弱灰
    "#c99c37": "#3d424a",  # 主干道 -> 雅致结构灰
    "#8a6820": "#282a30",  # 次干道 -> 暗灰色
    "#333530": "#1e2024",  # 支路步道
    "#272c2e": "#1c1d21",
    "#414033": "#1e2024",
    "#4f4b36": "#141517",
}


def smart_color_mapper(match):
    hex_color = match.group(0).lower()
    if hex_color in THEME_COLOR_MAP:
        return THEME_COLOR_MAP[hex_color]
    try:
        val = hex_color.lstrip("#")
        r, g, b = (int(val[i : i + 2], 16) for i in (0, 2, 4))
        lum = 0.299 * r + 0.587 * g + 0.114 * b
        return "#000000" if lum < 35 else "#25282e"
    except:
        return match.group(0)


# 执行精准替换
svg_content = re.sub(r"#[a-fA-F0-9]{6}\b", smart_color_mapper, svg_content)

# 净化底层：一键抹除所有原生遮罩、文字和线条
svg_content = re.sub(
    r"<defs>.*?</defs>", "", svg_content, flags=re.IGNORECASE | re.DOTALL
)
svg_content = re.sub(r'\s*mask="[^"]+"', "", svg_content, flags=re.IGNORECASE)
svg_content = re.sub(
    r"<text\b.*?</text>", "", svg_content, flags=re.IGNORECASE | re.DOTALL
)
svg_content = re.sub(r"<line\b.*?>", "", svg_content, flags=re.IGNORECASE | re.DOTALL)

# 💥 将 3D 山影图注入到底层黑色背景矩形上方、水系与路网下方
if has_hillshade and hillshade_svg_tag:
    bg_match = re.search(
        r'(<rect\s+width="[^"]+"\s+height="[^"]+"\s+fill="[^"]+"\s*/>)', svg_content
    )
    if bg_match:
        svg_content = svg_content.replace(
            bg_match.group(0), bg_match.group(0) + "\n" + hillshade_svg_tag
        )
    else:
        svg_content = re.sub(
            r"(<svg\b[^>]*>)", r"\1\n" + hillshade_svg_tag, svg_content, count=1
        )


# ==========================================
# 💥 2. 极简自适应排版 (带地理坐标装饰)
# ==========================================
text_color_fg = "#f0f0f0"

city_y_pos = height_px * 0.84
coord_y_pos = height_px * 0.865
stats_y_pos = height_px * 0.895
row_gap = height_px * 0.024

f_title = width_px * 0.055
f_coord = width_px * 0.013
f_large = width_px * 0.021
f_small = width_px * 0.016

# 城市大标题
city_letter_spacing = f"{width_px * 0.045:.1f}"
city_title_block = (
    f'<text x="{width_px / 2:.1f}" y="{city_y_pos:.1f}" font-family="Arial, Helvetica, sans-serif" '
    f'font-size="{f_title:.1f}" font-weight="bold" fill="{text_color_fg}" xml:space="preserve" '
    f'letter-spacing="{city_letter_spacing}" text-anchor="middle" opacity="0.95">{args.city.upper()}</text>\n'
)

# 艺术经纬度坐标与缓冲范围
lat_dir = "N" if args.lat >= 0 else "S"
lon_dir = "E" if args.lon >= 0 else "W"
coord_text = f"{abs(args.lat):.4f}° {lat_dir}   /   {abs(args.lon):.4f}° {lon_dir}   —   {args.distance / 1000:.1f} KM BUFFER"
coord_block = (
    f'<text x="{width_px / 2:.1f}" y="{coord_y_pos:.1f}" font-family="Arial, Helvetica, sans-serif" '
    f'font-size="{f_coord:.1f}" font-weight="normal" fill="{text_color_fg}" letter-spacing="{width_px * 0.008:.1f}" '
    f'text-anchor="middle" opacity="0.55">{coord_text}</text>\n'
)

pipe_str = f'<tspan xml:space="preserve" fill="{text_color_fg}" opacity="0.25" font-size="{f_large * 1.1:.1f}">   |   </tspan>'

# 第一行：只显示有数据的运动类型，避免 0 Rides 占位
row1_items = []
if run_count > 0:
    row1_items.append(
        f'<tspan font-weight="bold" font-size="{f_large:.1f}">{run_count}</tspan><tspan xml:space="preserve"> Runs </tspan><tspan font-weight="bold" font-size="{f_large:.1f}">{run_dist_km:.1f}</tspan><tspan xml:space="preserve"> km</tspan>'
    )
if ride_count > 0:
    row1_items.append(
        f'<tspan font-weight="bold" font-size="{f_large:.1f}">{ride_count}</tspan><tspan xml:space="preserve"> Rides </tspan><tspan font-weight="bold" font-size="{f_large:.1f}">{ride_dist_km:.1f}</tspan><tspan xml:space="preserve"> km</tspan>'
    )
if hike_count > 0:
    row1_items.append(
        f'<tspan font-weight="bold" font-size="{f_large:.1f}">{hike_count}</tspan><tspan xml:space="preserve"> Hikes </tspan><tspan font-weight="bold" font-size="{f_large:.1f}">{hike_dist_km:.1f}</tspan><tspan xml:space="preserve"> km</tspan>'
    )

row1_text = (
    pipe_str.join(row1_items)
    if row1_items
    else f"<tspan>{total_count} Workouts</tspan>"
)

# 第二行：心率与爬升
row2_items = []
if total_avg_hr > 30:
    row2_items.append(
        f'<tspan font-weight="bold" font-size="{f_large:.1f}">{int(total_avg_hr)}</tspan><tspan xml:space="preserve"> BPM Avg Heart Rate</tspan>'
    )
if total_elev_g > 0:
    row2_items.append(
        f'<tspan font-weight="bold" font-size="{f_large:.1f}">{int(total_elev_g)}</tspan><tspan xml:space="preserve"> m Elevation Gain</tspan>'
    )

row2_text = (
    pipe_str.join(row2_items)
    if row2_items
    else f'<tspan font-weight="bold">{total_dist_km:.1f}</tspan><tspan> km Total Distance</tspan>'
)

# 第三行：总运动次数与运动总时长
row3_text = (
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_count}</tspan><tspan xml:space="preserve"> Workouts Total </tspan>'
    f"{pipe_str}"
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_dist_km:.1f}</tspan><tspan xml:space="preserve"> km / </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_time_h}</tspan><tspan xml:space="preserve"> h </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_time_m}</tspan><tspan xml:space="preserve"> min</tspan>'
)

stats_block = (
    f'<g id="stats_block" transform="translate({width_px / 2:.1f}, {stats_y_pos:.1f})" fill="{text_color_fg}" '
    f'font-family="Arial, Helvetica, sans-serif" font-size="{f_small:.1f}" text-anchor="middle">\n'
    f'  <text transform="translate(0, 0)">{row1_text}</text>\n'
    f'  <text transform="translate(0, {row_gap:.1f})">{row2_text}</text>\n'
    f'  <text transform="translate(0, {row_gap * 2:.1f})">{row3_text}</text>\n'
    f"</g>\n"
)

# 最终注入并输出
final_injection = [
    "\n".join(svg_injection_lines),
    city_title_block,
    coord_block,
    stats_block,
]

if "</svg>" in svg_content:
    svg_content = svg_content.replace("</svg>", "\n".join(final_injection) + "\n</svg>")

final_path = "Workouts_Poster.svg"
with open(final_path, "w", encoding="utf-8") as f:
    f.write(svg_content)

print(f"\n🎉 大功告成！已合成海报：{final_path}")
