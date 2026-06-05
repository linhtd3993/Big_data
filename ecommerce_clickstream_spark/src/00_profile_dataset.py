from pathlib import Path
import pandas as pd

RAW_DIR = Path("ecommerce_clickstream_spark/data/raw/kaggle_original")
DOCS_DIR = Path("ecommerce_clickstream_spark/docs")
DOCS_DIR.mkdir(parents=True, exist_ok=True)

csv_files = sorted(RAW_DIR.glob("*.csv"))

if not csv_files:
    raise FileNotFoundError(f"No CSV files found in {RAW_DIR}")

summary_rows = []

for file_path in csv_files:
    df_sample = pd.read_csv(file_path, nrows=1000)

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        row_count = sum(1 for _ in f) - 1

    file_size_mb = file_path.stat().st_size / (1024 * 1024)

    summary_rows.append({
        "file_name": file_path.name,
        "rows": row_count,
        "columns": len(df_sample.columns),
        "size_mb": round(file_size_mb, 2),
        "column_names": ", ".join(df_sample.columns),
        "missing_values_in_first_1000_rows": int(df_sample.isna().sum().sum()),
    })

    print("=" * 80)
    print(f"File: {file_path.name}")
    print(f"Rows: {row_count}")
    print(f"Columns: {len(df_sample.columns)}")
    print(f"Size MB: {file_size_mb:.2f}")
    print("Columns:", list(df_sample.columns))
    print("Dtypes:")
    print(df_sample.dtypes)
    print("Missing values in first 1000 rows:")
    print(df_sample.isna().sum())

summary_df = pd.DataFrame(summary_rows)
summary_path = DOCS_DIR / "dataset_inventory.csv"
summary_df.to_csv(summary_path, index=False)

print("=" * 80)
print(f"Saved dataset inventory to: {summary_path}")
