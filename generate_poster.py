import argparse
import io
import math
import re
import sys
import urllib.request
from pathlib import Path

import duckdb
import numpy as np
from PIL import Image
from terraink_py import PosterRequest, generate_poster
from terraink_py.api import MercatorProjector

# 确保 matplotlib 可用（若环境中未安装则自动静默安装，保证运行不报错）
try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    import subprocess

    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "matplotlib", "--quiet"]
    )
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(description="生成运动轨迹海报")
parser.add_argument("--lat", type=float, required=True, help="中心点纬度")
parser.add_argument("--lon", type=float, required=True, help="中心点经度")
parser.add_argument("--distance", type=int, required=True, help="范围(米)")
parser.add_argument("--city", type=str, required=True, help="城市")
args = parser.parse_args()


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


# ==========================================
# 💥 极简矢量等高线生成函数（代替原平面色块） 💥
# ==========================================
def generate_contour_lines(bounds, project_func, distance_m, width_px):
    """基于全球 DEM 自动计算并生成极简细密矢量等高线"""
    # 依据范围动态选定高程瓦片层级
    zoom = (
        12
        if distance_m <= 10000
        else (11 if distance_m <= 30000 else (10 if distance_m <= 70000 else 9))
    )

    def deg2num(lat_deg, lon_deg, z):
        lat_rad = math.radians(lat_deg)
        n = 2.0**z
        xtile = int((lon_deg + 180.0) / 360.0 * n)
        ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return xtile, ytile

    def num2deg(xtile, ytile, z):
        n = 2.0**z
        lon_deg = xtile / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
        return math.degrees(lat_rad), lon_deg

    x_min, y_min = deg2num(bounds.north, bounds.west, zoom)
    x_max, y_max = deg2num(bounds.south, bounds.east, zoom)

    x_tiles = list(range(x_min, x_max + 1))
    y_tiles = list(range(y_min, y_max + 1))

    if len(x_tiles) * len(y_tiles) > 25:
        zoom -= 1
        x_min, y_min = deg2num(bounds.north, bounds.west, zoom)
        x_max, y_max = deg2num(bounds.south, bounds.east, zoom)
        x_tiles = list(range(x_min, x_max + 1))
        y_tiles = list(range(y_min, y_max + 1))

    tile_w, tile_h = 256, 256
    full_w = len(x_tiles) * tile_w
    full_h = len(y_tiles) * tile_h
    elev_grid = np.zeros((full_h, full_w), dtype=np.float32)

    downloaded = 0
    for i, x in enumerate(x_tiles):
        for j, y in enumerate(y_tiles):
            url = f"https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{zoom}/{x}/{y}.png"
            req = urllib.request.Request(
                url, headers={"User-Agent": "WorkoutsPoster/1.0"}
            )
            try:
                with urllib.request.urlopen(req, timeout=4) as resp:
                    img = Image.open(io.BytesIO(resp.read())).convert("RGB")
                    arr = np.array(img, dtype=np.float32)
                    tile_elev = (
                        arr[:, :, 0] * 256.0 + arr[:, :, 1] + arr[:, :, 2] / 256.0
                    ) - 32768.0
                    elev_grid[
                        j * tile_h : (j + 1) * tile_h, i * tile_w : (i + 1) * tile_w
                    ] = tile_elev
                    downloaded += 1
            except Exception:
                pass

    if downloaded == 0:
        return ""

    top_lat, left_lon = num2deg(x_min, y_min, zoom)
    bot_lat, right_lon = num2deg(x_max + 1, y_max + 1, zoom)

    lons = np.linspace(left_lon, right_lon, full_w)
    lats = np.linspace(bot_lat, top_lat, full_h)
    elev_grid = np.flipud(elev_grid)

    min_h = float(elev_grid.min())
    max_h = float(elev_grid.max())
    diff = max_h - min_h
    if diff < 30:
        return ""

    # 根据地势落差自适应等高距（米）
    interval = (
        20 if diff < 150 else (50 if diff < 400 else (100 if diff < 1000 else 200))
    )
    start = math.ceil(max(min_h, 10) / interval) * interval
    levels = np.arange(start, max_h, interval)
    if len(levels) == 0:
        return ""

    fig, ax = plt.subplots()
    cs = ax.contour(lons, lats, elev_grid, levels=levels)

    line_w = max(width_px * 0.00025, 0.45)
    lines_svg = [
        '<g id="minimal_contour_lines" fill="none" stroke-linecap="round" stroke-linejoin="round">'
    ]

    for level, segs in zip(cs.levels, cs.allsegs):
        is_index = int(level) % (interval * 5) == 0
        # 极简暗银灰：计曲线（大等高线）稍加深，普通等高线细腻微弱
        color = "#4a5260" if is_index else "#333842"
        opacity = 0.45 if is_index else 0.25
        cur_w = line_w * 1.3 if is_index else line_w

        for seg in segs:
            if len(seg) < 3:
                continue
            pixel_pts = []
            for pt in seg:
                px, py = project_func(pt[0], pt[1])
                pixel_pts.append(f"{px:.1f},{py:.1f}")
            pts_str = " ".join(pixel_pts)
            lines_svg.append(
                f'  <polyline points="{pts_str}" stroke="{color}" stroke-width="{cur_w:.2f}" stroke-opacity="{opacity:.2f}" />'
            )
    plt.close(fig)
    lines_svg.append("</g>")
    return "\n".join(lines_svg)


print(f"步骤 1/3：正在生成 {args.distance}m 范围的基础地图...")

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

print("步骤 2/3：读取并汇总运动数据...")

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

print("步骤 3/3：注入轨迹与排版...")

color_map = {
    "Run": "#FC4C02",
    "Cycling": "#22C55E",
    "Ride": "#22C55E",
    "Hike": "#FFC300",
    "Walk": "#A855F7",
}
default_color = "#06D6A0"
line_width = max(width_px * 0.0010, 0.75)

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
    '<g id="my_custom_tracks" fill="none" stroke-linecap="round" stroke-linejoin="round" opacity="0.95">'
]


def add_route_to_svg(lon_lat_list, m_type):
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
    svg_injection_lines.append(
        f'  <polyline points="{pts_str}" stroke="{color}" stroke-width="{line_width:.1f}" />'
    )


for r, t in other_routes:
    add_route_to_svg(r, t)
for r, t in run_routes:
    add_route_to_svg(r, t)
svg_injection_lines.append("</g>")

with open(result.files[0], "r", encoding="utf-8") as f:
    svg_content = f.read()


# ==========================================
# 💥 1. 精细化黑夜暗金滤镜（按图层精准着色） 💥
# ==========================================
THEME_COLOR_MAP = {
    # 陆地底色 -> 纯黑
    "#0a1628": "#000000",
    # 水系-> 深邃水体蓝
    "#061020": "#152b42",
    # 山体、林地面要素（原平铺色块消除为纯黑，改由极简矢量等高线呈现）
    "#0f2235": "#000000",
    # 建筑物面要素 -> 极暗微弱灰（消除市区高亮白斑噪声）
    "#6e5a45": "#181a1d",
    # 主干道 / 高速路 -> 适度结构的雅致灰
    "#c99c37": "#3d424a",
    # 次干道 -> 暗灰色
    "#8a6820": "#282a30",
    # 支路与步道 -> 极暗灰微弱纹理
    "#333530": "#1e2024",
    "#272c2e": "#1c1d21",
    "#414033": "#1e2024",
    "#4f4b36": "#141517",
}


def smart_color_mapper(match):
    hex_color = match.group(0).lower()
    if hex_color in THEME_COLOR_MAP:
        return THEME_COLOR_MAP[hex_color]
    # 其余未知颜色做兜底调暗
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

# 💥 将极简等高线注入在黑色陆地之上、水系道路之下
try:
    print("🏔️ 正在提取并生成山体极简矢量等高线...")
    contours_svg = generate_contour_lines(
        poster_bounds, project_func, args.distance, width_px
    )
    if contours_svg:
        svg_content = re.sub(
            r'(<rect\b[^>]+width="\d+"[^>]+height="\d+"[^>]*/>)',
            r"\1\n" + contours_svg,
            svg_content,
            count=1,
        )
        print("✅ 极简矢量等高线已成功融入底图！")
except Exception as e:
    print(f"⚠️ 生成等高线跳过: {e}")


# ==========================================
# 💥 2. 极简自适应排版 (纯黑背景下的白字排版) 💥
# ==========================================
text_color_fg = "#f0f0f0"

city_y_pos = height_px * 0.85
stats_y_pos = height_px * 0.885
row2_y = height_px * 0.027
row3_y = height_px * 0.053

f_large = width_px * 0.022
f_small = width_px * 0.018

# 渲染城市标题
city_letter_spacing = f"{width_px * 0.045:.1f}"
city_title_block = f'<text x="{width_px / 2:.1f}" y="{city_y_pos:.1f}" font-family="Arial, Helvetica, sans-serif" font-size="{width_px * 0.06:.1f}" font-weight="bold" fill="{text_color_fg}" xml:space="preserve" letter-spacing="{city_letter_spacing}" text-anchor="middle" opacity="0.9">{args.city.upper()}</text>\n'

# 内联的竖线分隔符
pipe_str = f'<tspan xml:space="preserve" fill="{text_color_fg}" opacity="0.25" font-size="{f_large * 1.1:.1f}">   |   </tspan>'

# 第一行
row1_text = (
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{run_count}</tspan><tspan xml:space="preserve"> Runs </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{run_dist_km:.1f}</tspan><tspan xml:space="preserve"> km</tspan>'
    f"{pipe_str}"
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{ride_count}</tspan><tspan xml:space="preserve"> Rides </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{ride_dist_km:.1f}</tspan><tspan xml:space="preserve"> km</tspan>'
    f"{pipe_str}"
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{hike_count}</tspan><tspan xml:space="preserve"> Hikes </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{hike_dist_km:.1f}</tspan><tspan xml:space="preserve"> km</tspan>'
)

# 第二行
row2_text = (
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{int(total_avg_hr)}</tspan><tspan xml:space="preserve"> BPM Avg Heart Rate</tspan>'
    f"{pipe_str}"
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{int(total_elev_g)}</tspan><tspan xml:space="preserve"> m Elevation Gain</tspan>'
)

# 第三行
row3_text = (
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_count}</tspan><tspan xml:space="preserve"> Workouts Total </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_dist_km:.1f}</tspan><tspan xml:space="preserve"> km / </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_time_h}</tspan><tspan xml:space="preserve"> h </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_time_m}</tspan><tspan xml:space="preserve"> min</tspan>'
)

# 将三行文本组合成块
stats_block = (
    f'<g id="stats_block" transform="translate({width_px / 2:.1f}, {stats_y_pos:.1f})" fill="{text_color_fg}" font-family="Arial, Helvetica, sans-serif" font-size="{f_small:.1f}" text-anchor="middle">\n'
    f'  <text transform="translate(0, 0)">{row1_text}</text>\n'
    f'  <text transform="translate(0, {row2_y:.1f})">{row2_text}</text>\n'
    f'  <text transform="translate(0, {row3_y:.1f})">{row3_text}</text>\n'
    f"</g>\n"
)

# 最终注入
final_injection = ["\n".join(svg_injection_lines), city_title_block, stats_block]

if "</svg>" in svg_content:
    svg_content = svg_content.replace("</svg>", "\n".join(final_injection) + "\n</svg>")

final_path = "Workouts_Poster.svg"
with open(final_path, "w", encoding="utf-8") as f:
    f.write(svg_content)

print(f"\n大功告成！路网已提亮的海报已生成：{final_path}")
