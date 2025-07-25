# 🏒 NHL Power BI Dashboard

This project scrapes real-time NHL data and turns it into a dynamic analytics dashboard using **Power BI**. 
> ⚠️ This project depends on the public [NHL API](https://api-web.nhle.com/v1/). If that API changes or breaks, this project will also break.

---

## 📣 **Author:** 
Developed by **Harsh Patel**  
If you would like to use this project for your own purposes, please do cite me.  
If you'd like to contribute — especially by adding advanced stats — feel free to fork the repo and open a pull request!

---
## ✅ Features

- Up-to-date NHL team standings
- Advanced team metrics (power play %, goals by period, faceoffs)
- Full rosters with player mugshots
- Player-level season and career stats (skater and goalie aware)
- Drillthrough support in Power BI by team and player

---

## 📸 Screenshots

| Home Dashboard | Team Page | Player Drillthrough (Skater View) | Player Drillthrough (Goalie View)|
|----------------|-----------|----------------------|----------------|
| ![home](nhl_powerbi_project/screenshots/OverallTeamsPage.jpg) | ![team](nhl_powerbi_project/screenshots/TeamStatsPage.jpg) | ![player](nhl_powerbi_project/screenshots/PlayerStatsPageSkaterView.jpg) | ![goalie](nhl_powerbi_project/screenshots/PlayerStatsPageGoalieView.jpg)|


---

## 📁 Project Structure

```
nhl_powerbi_project/
├── data/                   # Output folder for CSVs used by Power BI
│   ├── teams.csv
│   ├── rosters.csv
│   ├── player_stats.csv
|   ├── team_colors.csv
├── main.py                 # Python script to fetch data from NHL API
├── NHL_Dashboard.pbix      # Power BI Desktop report
README.md                   # This file
```

---

## 🧰 Installation

1. Make sure you have Python 3.8+ installed.
2. Install required packages:

```bash
pip install pandas requests urllib3
```

---

## 🔁 Season ID Handling

The NHL API uses **season IDs** (e.g., `"20242025"` for 2024–25).  
Due to offseason limitations, this project uses **two IDs**:

```python
SEASON_ID = "20252026"             # For current rosters (team pages)
SEASON_ID_OFFSEASON = "20242025"   # For season stats (power play %, etc.)
```

> When the regular season starts, change both to:
>
> ```python
> SEASON_ID = "20252026"
> SEASON_ID_OFFSEASON = "20252026"
> ```

---

## 🚀 How to Use This Project

### 1. Clone the repository

```bash
git clone https://github.com/HPatel999/NHL-Dashboard.git
cd nhl_powerbi_project
```

---

### 2. Run the data-fetching script

This will call the NHL API and save updated data to the `data/` folder:

```bash
python main.py
```

🟢 This will generate or overwrite:
- `teams.csv`
- `rosters.csv`
- `player_stats.csv`

---

### 3. Open the Power BI Report

Open `NHL-Dashboard.pbix` in Power BI Desktop.

---

### 4. Set the `BasePath` Parameter

The report uses a parameter (`BasePath`) to load CSV files from your local file system.

#### Set it up:

1. Go to **Home > Transform Data > Manage Parameters**
2. Set the value of `BasePath` to the full path of the `data/` folder, e.g.:

```
C:\Users\YourName\Documents\nhl_powerbi_project\data\
```

> ✅ Make sure it ends with a backslash (`\`)

3. Click **Close & Apply**

---

### 5. Refresh and explore

- ✅ All visuals should now load
- 🧠 Drill through team logos to see player tables (need to select a team before Ctrl + Clicking)
- 📊 Drill through player names (need to select a player and then right-click to see drillthrough option) to see full individual stats (with position-specific visuals)

---

## ⚠️ Known Limitations

- Data only updates when `main.py` is run manually and you also need to refresh the table in PowerBi pressing the Refresh Button
- If the NHL API changes or is rate-limited, some requests may fail
- Player images, logos, and data are fetched directly from NHL API (real-time dependencies)

---

## 📜 License & Attribution

- Data and images are provided by the **National Hockey League (NHL)** via their public API:  
  [https://api-web.nhle.com/v1/](https://api-web.nhle.com/v1/)
- All team logos, player headshots, and statistics are © NHL and respective teams.
- This project is **non-commercial and for educational purposes only**.

> © National Hockey League. All Rights Reserved.


