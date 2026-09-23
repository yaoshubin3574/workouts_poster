import argparse
import json
import math
import sys
import urllib.request
from pathlib import Path
import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_ACTIVITIES_URL = (
    "https://raw.githubusercontent.com/yaoshubin3574/workouts_page/master/src/static/activities.json"
)

parser = argparse.ArgumentParser(description="导出区域内的运动数据 (直连 workouts_page 数据源)")
parser.add_argument("--lat", type=float, required=True, help="中心点纬度")
parser.add_argument("--lon", type=float, required=True, help="中心点经度")
parser.add_argument("--distance", type=int, required=True, help="范围(米)")
parser.add_argument(
    "--data",
    type=str,
    default=DEFAULT_ACTIVITIES_URL,
    help="运动数据来源 (URL、JSON 文件路径 或 data.parquet 文件路径)",
)
parser.add_argument(
    "--output",
    type=str,
    default="regional_sports_data.csv",
    help="导出的 CSV 文件路径",
)
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
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


print(f"🔍 正在检索以 [{args.lat}, {args.lon}] 为中心，{args.distance}米为半径的运动数据...")

raw_items = []
source_str = str(args.data).strip()

if source_str.endswith(".parquet") and Path(source_str).exists():
    import duckdb

    sql = """
    SELECT summary_polyline, type, distance, moving_time, average_heartrate, elevation_gain 
    FROM read_parquet(?) 
    WHERE summary_polyline IS NOT NULL
    """
    with duckdb.connect() as conn:
        try:
            raw_rows = conn.execute(sql, [source_str]).fetchall()
            for r in raw_rows:
                raw_items.append(
                    {
                        "summary_polyline": r[0],
                        "type": r[1],
                        "distance": r[2],
                        "moving_time": r[3],
                        "average_heartrate": r[4],
                        "elevation_gain": r[5],
                    }
                )
        except Exception as e:
            print(f"❌ 读取 Parquet 失败: {e}")
            exit(1)
elif source_str.startswith("http://") or source_str.startswith("https://"):
    print(f"🌐 从远程拉取数据: {source_str} ...")
    req = urllib.request.Request(source_str, headers={"User-Agent": "workouts_poster/2.0"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw_items = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"❌ 远程获取失败: {e}")
        exit(1)
else:
    p = Path(source_str)
    if p.exists():
        print(f"📁 从本地读取数据: {p} ...")
        with open(p, "r", encoding="utf-8") as f:
            raw_items = json.load(f)
    else:
        print(f"❌ 找不到数据文件: {p}")
        exit(1)

filtered_data = []

for item in raw_items:
    poly_str = item.get("summary_polyline")
    if not poly_str:
        continue

    dist_m = safe_float(item.get("distance"))
    time_s = parse_time(item.get("moving_time"))
    avg_hr = safe_float(item.get("average_heartrate"))
    elev_g = safe_float(item.get("elevation_gain"))
    m_type = item.get("type", "Unknown")

    decoded_points = decode_polyline(poly_str)
    if not decoded_points or len(decoded_points) < 2:
        continue

    in_region = False
    for point in decoded_points:
        if haversine(point[0], point[1], args.lon, args.lat) <= args.distance:
            in_region = True
            break

    if in_region:
        filtered_data.append(
            {
                "运动类型 (Type)": m_type,
                "距离-米 (Distance)": round(dist_m, 2),
                "运动时间-秒 (Time)": int(time_s),
                "平均心率 (Avg HR)": round(avg_hr, 1),
                "海拔爬升-米 (Elevation)": round(elev_g, 2),
            }
        )

df = pd.DataFrame(filtered_data)
csv_filename = args.output
df.to_csv(csv_filename, index=False, encoding="utf-8-sig")

print(f"✅ 成功提取 {len(filtered_data)} 条记录，已导出至 {csv_filename}")
