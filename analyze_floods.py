import os
import sys
import pandas as pd
import matplotlib.pyplot as plt

DATA_PATH = "data/rainfall.xlsx"

# 1) Sanity check: make sure the file exists
if not os.path.exists(DATA_PATH):
    print(f"File not found: {DATA_PATH}\n"
          "Put your Excel file there or update DATA_PATH in analyze_floods.py.")
    sys.exit(1)

# 2) Load Excel, skipping metadata rows (HCFCD exports have 4 meta rows)
df = pd.read_excel(DATA_PATH, skiprows=4)

# 3) Clean up column names
clean_cols = [str(c).strip() for c in df.columns]
df.columns = clean_cols

print("\n[INFO] Columns after loading:")
print(df.columns.tolist()[:20], "... (truncated)\n")
print("[INFO] First 5 rows:")
print(df.head(), "\n")

# 4) Find a likely Date/Time column automatically
lower_cols = [c.lower() for c in df.columns]
# Heuristics: look for columns containing 'date' or 'time'
time_col = None
for c in df.columns:
    lc = c.lower()
    if "date" in lc or "time" in lc:
        time_col = c
        break

# Fallback: assume the first column is time if nothing matched
if time_col is None:
    time_col = df.columns[0]
    print(f"[WARN] No explicit Date/Time column found. Using first column: '{time_col}'.")
else:
    print(f"[INFO] Using time column: '{time_col}'.")

# 5) Parse time and drop rows without valid timestamps
df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
df = df.dropna(subset=[time_col])

# 6) Drop columns that are entirely NaN (common in wide sensor exports)
df = df.dropna(axis=1, how='all')

# 7) Identify site columns (everything except the time column)
site_cols = [c for c in df.columns if c != time_col]

# Convert all site columns to numeric if possible
for c in site_cols:
    df[c] = pd.to_numeric(df[c], errors='coerce')

# Pick the first site column that actually has data
site_col = None
for c in site_cols:
    if df[c].notna().any():
        site_col = c
        break

if site_col is None:
    print("[ERROR] Could not find any non-empty site columns after cleaning.\n"
          "Inspect the Excel file to confirm where the rainfall values are (sheet, rows).")
    sys.exit(1)

print(f"[INFO] Plotting site column: '{site_col}'\n")

# 8) Plot
df_sorted = df.sort_values(by=time_col)
plt.figure(figsize=(10, 5))
plt.plot(df_sorted[time_col], df_sorted[site_col], label=f"{site_col} (inches)")
plt.xlabel("Date/Time")
plt.ylabel("Rainfall (inches)")
plt.title(f"Rainfall Over Time — {site_col}")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()

# Save and show
out_path = "outputs"
os.makedirs(out_path, exist_ok=True)
fig_path = os.path.join(out_path, f"rainfall_{site_col}.png")
plt.savefig(fig_path)
print(f"[INFO] Saved plot to: {fig_path}")
plt.show()