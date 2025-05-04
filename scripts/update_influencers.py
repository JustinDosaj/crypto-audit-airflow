import pandas as pd
from pathlib import Path

parquet_path = Path("./data/influencer_seed.parquet")

# Load existing data or create empty DataFrame
if parquet_path.exists():
    df = pd.read_parquet(parquet_path)
else:
    df = pd.DataFrame(columns=["username", "last_scanned"])

# Add new influencer(s)
new_data = pd.DataFrame([
    {"username": "JakeGagain", "last_scanned": None},
    {"username": "TheCryptoLark", "last_scanned": None},
])

# Combine, drop duplicates
df = pd.concat([df, new_data]).drop_duplicates(subset="username")

# Save back
df.to_parquet(parquet_path, index=False)