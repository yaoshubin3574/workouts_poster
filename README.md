# Workouts Poster 🏃🚴🥾

自动生成高精度、极简黑金艺术风格的运动轨迹城市海报（支持 SVG 矢量格式与 300 PPI 印刷级高清 PNG）。

本项目已深度打通 **[workouts_page](https://github.com/yaoshubin3574/workouts_page)**，海报生成时**直接从 `workouts_page` 的 `activities.json` 实时获取最新运动轨迹**，无需在仓库内维护任何 `.gpx` 轨迹文件，运动同步自动化无缝衔接。

---

## ✨ 核心特性

- 🔗 **实时直连 workouts_page**：自动读取 `src/static/activities.json`，支持 Google Polyline 原始几何高密度坐标解码，精度与 GPX 完全一致。
- 🖤 **精细化黑金主题滤镜**：纯黑背景配合深蓝水系、墨绿自然公园、暗灰建筑网格，彻底消除市区噪点。
- 🗺️ **OSM 复杂水系拓扑拼接**：针对西湖、江河、海湾海峡等多边形关系（Multipolygon）实现自适应岸线链式缝合，杜绝漏填与撕裂。
- 🎨 **极简排版布局**：自适应呈现大写城市标题，精确统计分类型运动里程（Runs / Rides / Hikes）、时间加权平均心率、累计爬升与总运动时长。
- 🚀 **GitHub Actions 云端全自动**：预设 8+ 热门城市，支持一键批量生成、Inkscape 渲染 300 PPI PNG 并自动发布至 GitHub Releases。

---

## 🚀 使用指南

### 1. GitHub Actions 云端一键生成（推荐）

1. 进入仓库的 **Actions** 标签页。
2. 选择 **Workouts Poster Generator** 工作流。
3. 点击 **Run workflow**：
   - **预设城市**：选择 `ALL CITIES`（批量生成所有预设城市）或单个城市（如 `HANGZHOU`、`HENAN`、`JIAOZUO`、`JINHUA`、`NINGBO`、`TAIZHOU`、`XINXIANG` 等）。
   - **自定义模式**：选择 `CUSTOM`，并在下方输入自定义的 `city`、`lat`、`lon`、`distance`。
4. 构建完成后，直接在 **Releases** 页面下载 300 PPI 的超清海报 PNG，或在 **Artifacts** 下载打包产物。

---

## 2. 本地命令行运行

#### 安装依赖
```bash
pip install -r requirements.txt
# 或者
pip install Pillow terraink-py pandas pyarrow duckdb
```

#### 生成指定城市海报
```bash
# 默认自动从 workouts_page 远程拉取最新运动数据
python generate_poster.py --city HANGZHOU --lat 30.2250866 --lon 120.1390416 --distance 4600

# 也可以指定本地下载好的 activities.json
python generate_poster.py --city JIAOZUO --lat 35.1946971 --lon 113.2526282 --distance 13000 --activities activities.json
```

#### 导出特定区域运动数据统计 CSV
```bash
# 导出杭州西湖周边 4600 米内的运动明细报表
python export_regional_data.py --lat 30.2250866 --lon 120.1390416 --distance 4600 --output hangzhou_sports.csv
```

---

## 🔗 相关项目

* [workouts_page (当前数据源)](https://github.com/yaoshubin3574/workouts_page)
* [running_page](https://github.com/yihong0618/running_page)
* [terraink_py](https://github.com/yihong0618/terraink_py)
