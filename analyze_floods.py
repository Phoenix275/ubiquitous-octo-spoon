import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DATA_PATH = "data/rainfall.xlsx"
OUTPUT_DIR = "outputs"
TOP_N = 2  # auto-select this many gauges for figures
STATION_MAP_PATH = "data/stations.csv"  

# --- Event window (set to your storm dates; leave as-is to disable) ---
EVENT_START = "2024-05-15 00:00"  # e.g., start of a May storm
EVENT_END   = "2024-05-20 23:59"  # e.g., end of a May storm
ENABLE_EVENT_WINDOW = True

# --------------------- helpers ---------------------
def log(msg: str):
    print(f"[INFO] {msg}")

def warn(msg: str):
    print(f"[WARN] {msg}")

def err(msg: str):
    print(f"[ERROR] {msg}")
    sys.exit(1)

def ensure_outdir(path: str):
    os.makedirs(path, exist_ok=True)

# ------------------ load & normalize ------------------
if not os.path.exists(DATA_PATH):
    err(f"File not found: {DATA_PATH}. Put your Excel export there or update DATA_PATH.")

# Read without trusting headers; detect header row
raw = pd.read_excel(DATA_PATH, header=None)

# --- header row detection tuned for HCFCD exports ---
header_row = None
max_scan = min(50, len(raw))
for i in range(max_scan):
    row_str = raw.iloc[i].astype(str).str.strip().str.lower()
    if (row_str == 'data_time_utc').any() or row_str.str.contains('data_time').any():
        header_row = i
        break

if header_row is None:
    for i in range(max_scan):
        nonnull = raw.iloc[i].dropna()
        if len(nonnull) == 0:
            continue
        first_val = str(nonnull.iloc[0]).strip().lower()
        if 'data_time' in first_val or 'date/time' in first_val:
            header_row = i
            break

if header_row is None:
    header_row = 3
    warn(f"No explicit header found; falling back to row {header_row}.")

log(f"Using header_row={header_row}")

# Re-read with detected header
df = pd.read_excel(DATA_PATH, header=header_row)

log(f"Raw columns with header_row={header_row}: {list(df.columns)[:12]} ...")

# Drop fully empty rows
df = df.dropna(how='all')

# Clean column names and drop any 'Unnamed:*' placeholders
cols = [str(c).strip() for c in df.columns]
df.columns = cols
df = df.loc[:, [c for c in df.columns if not str(c).lower().startswith('unnamed')]]
log(f"Cleaned columns (first 12): {list(df.columns)[:12]} ...")

# Identify time column
time_col = None
for c in df.columns:
    cl = str(c).strip().lower()
    if cl == 'data_time_utc':
        time_col = c
        break
if time_col is None:
    for c in df.columns:
        cl = str(c).strip().lower()
        if 'data_time' in cl or 'date/time' in cl or (('date' in cl) and ('time' in cl)):
            time_col = c
            break
if time_col is None:
    for c in df.columns:
        cl = str(c).lower()
        if 'date' in cl or 'time' in cl:
            time_col = c
            break
if time_col is None:
    time_col = df.columns[0]
    warn(f"No explicit Date/Time column found. Using first column: '{time_col}'.")

if str(time_col).lower().startswith('unnamed'):
    df.rename(columns={time_col: 'DateTime'}, inplace=True)
    time_col = 'DateTime'

log(f"Using time column: {time_col}")

# Parse time and drop invalid timestamps
df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
df = df.dropna(subset=[time_col])

# Convert remaining columns to numeric rainfall values
site_cols = [c for c in df.columns if c != time_col and not str(c).lower().startswith("unnamed")]
if not site_cols:
    err("No site columns found after cleaning. Check the Excel export.")
for c in site_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# Drop empty site columns
site_cols = [c for c in site_cols if df[c].notna().any()]
if not site_cols:
    err("All site columns are empty after numeric coercion.")

# Sort by time
df = df.sort_values(by=time_col)

# ------------------ station mapping (NEW) ------------------
station_map = {}
if os.path.exists(STATION_MAP_PATH):
    m = pd.read_csv(STATION_MAP_PATH)
    station_map = dict(zip(m["site_id"], m["name"]))
    log(f"Loaded {len(station_map)} station name mappings.")

# ------------------ choose best gauges ------------------
quality_rows = []
for c in site_cols:
    s = df[c]
    completeness = float(s.notna().mean())  # 0..1
    variability = float(s.std(skipna=True)) if s.notna().any() else 0.0
    score = completeness * 0.7 + variability * 0.3
    quality_rows.append((c, completeness, variability, score))

quality = pd.DataFrame(quality_rows, columns=["site", "completeness", "std", "score"]).sort_values(
    by=["score", "completeness", "std"], ascending=False
)
log("Top gauges by quality score:\n" + quality.head(10).to_string(index=False))

chosen = quality["site"].head(TOP_N).tolist()
if len(chosen) == 0:
    err("Could not select gauges — empty ranking.")
log(f"Auto-selected gauges: {chosen}")

# Ensure we never plot an 'Unnamed' column by mistake
chosen = [c for c in chosen if not str(c).lower().startswith("unnamed")]
if not chosen:
    err("Selection resulted in only 'Unnamed' columns. Check header detection.")
log(f"Final chosen gauges: {chosen}")

# ------------------ resample & summaries ------------------
work = df[[time_col] + chosen].copy().set_index(time_col).sort_index()

# Resample hourly and compute cumulative
hourly = work.resample('h').sum(min_count=1)   # lowercase 'h' avoids FutureWarning
cum = hourly.cumsum()

summary = pd.DataFrame({
    "total_inches": hourly.sum(),
    "max_hour": hourly.max(),
    "max_6h": hourly.rolling(6).sum().max(),
    "max_24h": hourly.rolling(24).sum().max(),
}).round(3)

ensure_outdir(OUTPUT_DIR)
summary_path = os.path.join(OUTPUT_DIR, "summary_selected_gauges.csv")
quality_path = os.path.join(OUTPUT_DIR, "gauge_quality.csv")
hourly_path = os.path.join(OUTPUT_DIR, "hourly_selected_gauges.csv")
cum_path = os.path.join(OUTPUT_DIR, "cumulative_selected_gauges.csv")

summary.to_csv(summary_path)
quality.to_csv(quality_path, index=False)
hourly.to_csv(hourly_path)
cum.to_csv(cum_path)

log(f"Wrote: {summary_path}")
log(f"Wrote: {quality_path}")
log(f"Wrote: {hourly_path}")
log(f"Wrote: {cum_path}")

# ------------------ event-window analysis (optional) ------------------
if ENABLE_EVENT_WINDOW:
    try:
        evt = hourly.loc[EVENT_START:EVENT_END].copy()
    except Exception as e:
        warn(f"Could not slice event window: {e}")
        evt = pd.DataFrame()

    if not evt.empty:
        evt_summary = pd.DataFrame({
            "event_total": evt.sum(),
            "event_max_hour": evt.max(),
            "event_max_6h": evt.rolling(6).sum().max(),
            "event_max_24h": evt.rolling(24).sum().max(),
            "peak_hour_time": evt.idxmax(),
        }).round(3)

        evt_path = os.path.join(OUTPUT_DIR, "event_summary.csv")
        evt_summary.to_csv(evt_path)
        log(f"Wrote: {evt_path}")

        # Event plots (hourly + cumulative)
        evt_cum = evt.cumsum()

        # Hourly plot (event window)
        plt.figure(figsize=(10, 5))
        for c in evt.columns:
            label = station_map.get(c, c)
            plt.plot(evt.index, evt[c], label=f"{label} (hourly)")
        plt.title(f"Hourly Rainfall — Event Window {EVENT_START} to {EVENT_END}")
        plt.xlabel("Date/Time"); plt.ylabel("Rainfall (inches)")
        plt.legend(); plt.xticks(rotation=45); plt.tight_layout()
        safe_names_evt = [station_map.get(c, c).replace(" ", "_").replace("@", "at") for c in evt.columns]
        f_evt_h = os.path.join(OUTPUT_DIR, f"hourly_event_{'_vs_'.join(safe_names_evt)}.png")
        plt.savefig(f_evt_h); log(f"Saved plot: {f_evt_h}")
        plt.show()

        # Cumulative plot (event window)
        plt.figure(figsize=(10, 5))
        for c in evt_cum.columns:
            label = station_map.get(c, c)
            plt.plot(evt_cum.index, evt_cum[c], label=f"{label} (cumulative)")
        plt.title(f"Cumulative Rainfall — Event Window {EVENT_START} to {EVENT_END}")
        plt.xlabel("Date/Time"); plt.ylabel("Cumulative Rainfall (inches)")
        plt.legend(); plt.xticks(rotation=45); plt.tight_layout()
        f_evt_c = os.path.join(OUTPUT_DIR, f"cumulative_event_{'_vs_'.join(safe_names_evt)}.png")
        plt.savefig(f_evt_c); log(f"Saved plot: {f_evt_c}")
        plt.show()
    else:
        warn(f"No data found in event window {EVENT_START} to {EVENT_END}.")

# ------------------ figures (matplotlib only) ------------------
# Hourly overlay
plt.figure(figsize=(10, 5))
for c in chosen:
    label = station_map.get(c, c)  # friendly name if available
    plt.plot(hourly.index, hourly[c], label=f"{label} (hourly)")
plt.xlabel("Date/Time")
plt.ylabel("Rainfall (inches)")
plt.title("Hourly Rainfall — Selected Gauges")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
safe_names = [station_map.get(c, c).replace(" ", "_").replace("@", "at") for c in chosen]
fig1 = os.path.join(OUTPUT_DIR, f"hourly_{'_vs_'.join(safe_names)}.png")
plt.savefig(fig1)
log(f"Saved plot: {fig1}")
plt.show()

# Cumulative overlay
plt.figure(figsize=(10, 5))
for c in chosen:
    label = station_map.get(c, c)
    plt.plot(cum.index, cum[c], label=f"{label} (cumulative)")
plt.xlabel("Date/Time")
plt.ylabel("Cumulative Rainfall (inches)")
plt.title("Cumulative Rainfall — Selected Gauges")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
safe_names = [station_map.get(c, c).replace(" ", "_").replace("@", "at") for c in chosen]
fig2 = os.path.join(OUTPUT_DIR, f"cumulative_{'_vs_'.join(safe_names)}.png")
plt.savefig(fig2)
log(f"Saved plot: {fig2}")
plt.show()

log("Done.")