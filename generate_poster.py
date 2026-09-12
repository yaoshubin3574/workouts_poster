import argparse
import math
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

# 保证在任何环境下控制台输出中文与 Emoji 正常
sys.stdout.reconfigure(encoding="utf-8")

# =========================================================
# 🛠️ 补丁：修复 terraink-py 无法正确解析大型水体(西湖/大江大河)的 Bug
# 将 OSM Multipolygon 关系中拆碎的岸线分段自动缝合成完整闭合的大水系
# =========================================================
import terraink_py.osm as _osm
from terraink_py import PosterRequest, generate_poster

_orig_extract_paths = _osm.extract_paths


def _stitch_open_ways(ways, tol=1e-4):
    """将首尾相接的未闭合线段缝合成完整的闭合多边形外圈"""
    if not ways:
        return []
    closed_rings, open_ways = [], []
    for w in ways:
        if len(w) < 2:
            continue
        if (
            len(w) >= 4
            and (w[0][0] - w[-1][0]) ** 2 + (w[0][1] - w[-1][1]) ** 2 <= tol**2
        ):
            closed_rings.append(w)
        else:
            open_ways.append(list(w))

    tol_sq = tol**2
    while open_ways:
        current = open_ways.pop(0)
        extended = True
        while extended:
            extended = False
            if (
                len(current) >= 4
                and (current[0][0] - current[-1][0]) ** 2
                + (current[0][1] - current[-1][1]) ** 2
                <= tol_sq
            ):
                current[-1] = current[0]
                closed_rings.append(current)
                break
            c_end, c_start = current[-1], current[0]
            matched_idx, match_type = -1, None
            for i, other in enumerate(open_ways):
                o_start, o_end = other[0], other[-1]
                if (c_end[0] - o_start[0]) ** 2 + (
                    c_end[1] - o_start[1]
                ) ** 2 <= tol_sq:
                    matched_idx, match_type = i, "append_forward"
                    break
                elif (c_end[0] - o_end[0]) ** 2 + (c_end[1] - o_end[1]) ** 2 <= tol_sq:
                    matched_idx, match_type = i, "append_reverse"
                    break
                elif (c_start[0] - o_end[0]) ** 2 + (
                    c_start[1] - o_end[1]
                ) ** 2 <= tol_sq:
                    matched_idx, match_type = i, "prepend_forward"
                    break
                elif (c_start[0] - o_start[0]) ** 2 + (
                    c_start[1] - o_start[1]
                ) ** 2 <= tol_sq:
                    matched_idx, match_type = i, "prepend_reverse"
                    break
            if matched_idx != -1:
                other = open_ways.pop(matched_idx)
                if match_type == "append_forward":
                    current.extend(other[1:])
                elif match_type == "append_reverse":
                    current.extend(other[-2::-1])
                elif match_type == "prepend_forward":
                    current = other[:-1] + current
                elif match_type == "prepend_reverse":
                    current = other[:0:-1] + current
                extended = True
            if not extended:
                break
        if len(current) >= 4:
            current.append(current[0])
            closed_rings.append(current)
    return closed_rings


def _patched_extract_paths(element: dict, *, polygon: bool):
    """拦截 relation 解析，使用真正的岸线拼接逻辑"""
    if element.get("type") == "relation" and polygon:
        members = [
            m
            for m in element.get("members", [])
            if m.get("type") == "way" and m.get("geometry")
        ]
        preferred = [m for m in members if m.get("role") == "outer"] or [
            m for m in members if m.get("role") != "inner"
        ]
        ways = [_osm.geometry_to_points(m.get("geometry", [])) for m in preferred]
        return _stitch_open_ways(ways)
    return _orig_extract_paths(element, polygon=polygon)


_osm.extract_paths = _patched_extract_paths
# =========================================================
from terraink_py.api import MercatorProjector

parser = argparse.ArgumentParser(description="生成运动轨迹海报")
parser.add_argument("--lat", type=float, required=True, help="中心点纬度")
parser.add_argument("--lon", type=float, required=True, help="中心点经度")
parser.add_argument("--distance", type=int, required=True, help="范围(米)")
parser.add_argument("--city", type=str, required=True, help="城市代码或标识")
parser.add_argument(
    "--title", type=str, default=None, help="海报上显示的城市标题(默认等于--city)"
)
parser.add_argument(
    "--output", type=str, default="Workouts_Poster.svg", help="输出SVG文件路径"
)
args = parser.parse_args()

display_title = (args.title if args.title else args.city).strip()


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


def parse_iso_time(t_str):
    if not t_str:
        return None
    t_str = t_str.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            return datetime.strptime(t_str.replace("+00:00", "Z"), fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(t_str.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_gpx_file(file_path):
    """解析单个 GPX 原始文件，提取完整高精度轨迹点及运动统计数据"""
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
    except Exception as e:
        print(f"⚠️ 无法读取 GPX 文件 {file_path}: {e}")
        return []

    if not content.strip():
        return []

    root = None
    try:
        root = ET.fromstring(content)
    except Exception:
        pass

    if root is None:
        try:
            cleaned = re.sub(r'\s+xmlns(?::\w+)?=["\'][^"\']*["\']', "", content)
            cleaned = re.sub(r'\s+\w+:\w+=["\'][^"\']*["\']', "", cleaned)
            cleaned = re.sub(r"(</?)\w+:", r"\1", cleaned)
            root = ET.fromstring(cleaned)
        except Exception as e:
            print(f"⚠️ 解析 GPX XML 失败 {file_path}: {e}")
            return []

    for elem in root.iter():
        if isinstance(elem.tag, str) and "}" in elem.tag:
            elem.tag = elem.tag.split("}", 1)[1]

    activities = []
    trks = root.findall(".//trk")
    if not trks:
        trks = root.findall(".//rte") or [root]

    stem_name = Path(file_path).stem.lower()
    for trk in trks:
        trk_type = (
            (
                trk.findtext("type")
                or root.findtext(".//type")
                or trk.findtext("name")
                or root.findtext(".//name")
                or stem_name
            )
            .strip()
            .lower()
        )

        if any(k in trk_type for k in ["cycl", "ride", "bike", "velo"]):
            m_type = "Cycling"
        elif any(k in trk_type for k in ["hike", "hiking", "mount", "walk"]):
            m_type = "Hike"  # 💥 walk 与 hike 统一归入 Hike 运动类别
        else:
            m_type = "Run"

        points = []
        times = []
        elevations = []
        hrs = []

        pts = trk.findall(".//trkpt") or trk.findall(".//rtept")
        for pt in pts:
            try:
                lat_str = pt.get("lat") or pt.get("latitude") or pt.get("Lat")
                lon_str = (
                    pt.get("lon")
                    or pt.get("lng")
                    or pt.get("longitude")
                    or pt.get("Lon")
                )
                if lat_str is None or lon_str is None:
                    continue
                lat = float(lat_str)
                lon = float(lon_str)
                points.append([lon, lat])
            except (KeyError, ValueError):
                continue

            ele_str = pt.findtext("ele")
            if ele_str:
                try:
                    elevations.append(float(ele_str))
                except ValueError:
                    pass

            t_str = pt.findtext("time")
            if t_str:
                times.append(t_str.strip())

            hr_str = pt.findtext(".//hr")
            if hr_str:
                try:
                    hrs.append(float(hr_str))
                except ValueError:
                    pass

        if len(points) < 2:
            continue

        dist_m = 0.0
        for i in range(len(points) - 1):
            dist_m += haversine(
                points[i][0], points[i][1], points[i + 1][0], points[i + 1][1]
            )

        time_s = 0.0
        if len(times) >= 2:
            t_start = parse_iso_time(times[0])
            t_end = parse_iso_time(times[-1])
            if t_start and t_end:
                time_s = max(0.0, (t_end - t_start).total_seconds())

        elev_g = 0.0
        for i in range(len(elevations) - 1):
            diff = elevations[i + 1] - elevations[i]
            if diff > 0:
                elev_g += diff

        avg_hr = (sum(hrs) / len(hrs)) if hrs else 0.0

        activities.append((points, m_type, dist_m, time_s, avg_hr, elev_g))

    return activities


print(f"步骤 1/3：正在生成 {args.distance}m 范围的基础地图...")

result = generate_poster(
    PosterRequest(
        output=Path(f"./base-map-{args.city}"),
        formats=("svg",),
        lat=args.lat,
        lon=args.lon,
        title=display_title,
        subtitle="",
        theme="dark",
        width_cm=21,
        height_cm=29.7,
        distance_m=args.distance,
        include_buildings=True,
    )
)

print("步骤 2/3：从 .gpx 目录读取并汇总原始运动数据...")

poster_bounds = result.bounds.poster_bounds
width_px = result.size.width
height_px = result.size.height
projector = MercatorProjector.from_bounds(poster_bounds, width_px, height_px)

gpx_candidates = [Path(".gpx"), Path("GPX"), Path("gpx"), Path("data")]
gpx_files = []
found_dirs = []

for d in gpx_candidates:
    if d.exists() and d.is_dir():
        found = [p for p in d.rglob("*") if p.is_file() and p.suffix.lower() == ".gpx"]
        if found:
            gpx_files.extend(found)
            found_dirs.append(d.name)

if not gpx_files:
    for p in Path(".").rglob("*"):
        if p.is_file() and p.suffix.lower() == ".gpx" and ".git" not in p.parts:
            gpx_files.append(p)
    if gpx_files:
        found_dirs.append("workspace")

gpx_files = sorted(list(set(gpx_files)))

workout_records = []
if gpx_files:
    dir_info = "/".join(set(found_dirs)) if found_dirs else "unknown"
    print(
        f"📁 成功从 [{dir_info}] 扫描到 {len(gpx_files)} 个 GPX 文件，开始解析轨迹..."
    )
    for gpx_file in gpx_files:
        workout_records.extend(parse_gpx_file(gpx_file))
    print(f"✅ 成功加载 {len(workout_records)} 条运动轨迹。")
else:
    print("⚠️ 警告：在 .gpx / GPX 目录下未找到任何 .gpx 文件！")

print("步骤 3/3：注入轨迹与排版...")

color_map = {
    "Run": "#FC4C02",
    "Cycling": "#22C55E",
    "Ride": "#22C55E",
    "Hike": "#FFC300",
    "Walk": "#FFC300",
}
default_color = "#06D6A0"
line_width = max(width_px * 0.0010, 0.75)

run_count = ride_count = hike_count = total_count = 0
run_dist_km = ride_dist_km = hike_dist_km = total_dist_km = 0
total_elev_g = total_weighted_hr = total_time_s = 0

run_routes, other_routes = [], []
pb = poster_bounds

for points, m_type, dist_m, time_s, avg_hr, elev_g in workout_records:
    if not points or len(points) < 2:
        continue

    in_region = False
    for point in points:
        p_lon, p_lat = point[0], point[1]
        if haversine(p_lon, p_lat, args.lon, args.lat) <= args.distance or (
            pb.west <= p_lon <= pb.east and pb.south <= p_lat <= pb.north
        ):
            in_region = True
            break

    if not in_region:
        continue

    if m_type == "Run":
        run_routes.append((points, m_type))
        run_count += 1
        run_dist_km += dist_m / 1000.0
    elif m_type in ["Cycling", "Ride"]:
        other_routes.append((points, m_type))
        ride_count += 1
        ride_dist_km += dist_m / 1000.0
    else:  # Hike 与 Walk 全部合并统计到 Hike
        other_routes.append((points, "Hike"))
        hike_count += 1
        hike_dist_km += dist_m / 1000.0

    total_count += 1
    total_dist_km += dist_m / 1000.0
    total_elev_g += elev_g
    total_weighted_hr += avg_hr * time_s
    total_time_s += time_s

print(
    f"📍 匹配到当前海报区域（{display_title} 半径 {args.distance}m）的运动记录：{total_count} 条 "
    f"（跑步: {run_count}, 骑行: {ride_count}, 徒步/步行: {hike_count}）"
)

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
        x, y = projector.project(lon, lat)
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

THEME_COLOR_MAP = {
    "#0a1628": "#000000",
    "#061020": "#152b42",
    "#0f2235": "#0a120e",
    "#6e5a45": "#181a1d",
    "#c99c37": "#3d424a",
    "#8a6820": "#282a30",
    "#333530": "#1e2024",
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


svg_content = re.sub(r"#[a-fA-F0-9]{6}\b", smart_color_mapper, svg_content)
svg_content = re.sub(
    r"<defs>.*?</defs>", "", svg_content, flags=re.IGNORECASE | re.DOTALL
)
svg_content = re.sub(r'\s*mask="[^"]+"', "", svg_content, flags=re.IGNORECASE)
svg_content = re.sub(
    r"<text\b.*?</text>", "", svg_content, flags=re.IGNORECASE | re.DOTALL
)
svg_content = re.sub(r"<line\b.*?>", "", svg_content, flags=re.IGNORECASE | re.DOTALL)

text_color_fg = "#f0f0f0"
city_y_pos = height_px * 0.85
stats_y_pos = height_px * 0.885
row2_y = height_px * 0.027
row3_y = height_px * 0.053

f_large = width_px * 0.022
f_small = width_px * 0.018

city_letter_spacing = f"{width_px * 0.045:.1f}"
city_title_block = f'<text x="{width_px / 2:.1f}" y="{city_y_pos:.1f}" font-family="Arial, Helvetica, sans-serif" font-size="{width_px * 0.06:.1f}" font-weight="bold" fill="{text_color_fg}" xml:space="preserve" letter-spacing="{city_letter_spacing}" text-anchor="middle" opacity="0.9">{display_title.upper()}</text>\n'

pipe_str = f'<tspan xml:space="preserve" fill="{text_color_fg}" opacity="0.25" font-size="{f_large * 1.1:.1f}">   |   </tspan>'

# 💥 第一行：Walk 与 Hike 合并展示在 Hikes 项中
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

row2_text = (
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{int(total_avg_hr)}</tspan><tspan xml:space="preserve"> BPM Avg Heart Rate</tspan>'
    f"{pipe_str}"
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{int(total_elev_g)}</tspan><tspan xml:space="preserve"> m Elevation Gain</tspan>'
)

row3_text = (
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_count}</tspan><tspan xml:space="preserve"> Workouts Total </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_dist_km:.1f}</tspan><tspan xml:space="preserve"> km / </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_time_h}</tspan><tspan xml:space="preserve"> h </tspan>'
    f'<tspan font-weight="bold" font-size="{f_large:.1f}">{total_time_m}</tspan><tspan xml:space="preserve"> min</tspan>'
)

stats_block = (
    f'<g id="stats_block" transform="translate({width_px / 2:.1f}, {stats_y_pos:.1f})" fill="{text_color_fg}" font-family="Arial, Helvetica, sans-serif" font-size="{f_small:.1f}" text-anchor="middle">\n'
    f'  <text transform="translate(0, 0)">{row1_text}</text>\n'
    f'  <text transform="translate(0, {row2_y:.1f})">{row2_text}</text>\n'
    f'  <text transform="translate(0, {row3_y:.1f})">{row3_text}</text>\n'
    f"</g>\n"
)

final_injection = ["\n".join(svg_injection_lines), city_title_block, stats_block]

if "</svg>" in svg_content:
    svg_content = svg_content.replace("</svg>", "\n".join(final_injection) + "\n</svg>")

final_path = args.output if args.output else "Workouts_Poster.svg"
Path(final_path).parent.mkdir(parents=True, exist_ok=True)
with open(final_path, "w", encoding="utf-8") as f:
    f.write(svg_content)

print(f"\n大功告成！海报已成功生成：{final_path}")
