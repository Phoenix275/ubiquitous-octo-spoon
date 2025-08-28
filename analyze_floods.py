import pandas as pd
import matplotlib.pyplot as plt

# Load rainfall data from Excel (replace with your file path)
df = pd.read_excel("data/rainfall.xlsx")

# Inspect the first few rows
print(df.head())

# Simple plot: rainfall over time
plt.figure(figsize=(10,5))
plt.plot(df["Date"], df["Rainfall"], label="Rainfall (inches)")
plt.xlabel("Date")
plt.ylabel("Rainfall (inches)")
plt.title("Rainfall Over Time - Hurricane Harvey Example")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()