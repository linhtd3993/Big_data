from pathlib import Path


# Lay thu muc goc cua project dua tren vi tri file nay.
# Cach nay giup script chay dung du duoc goi tu terminal hay IDE.
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Cac duong dan dung chung cho toan bo pipeline.
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw" / "kaggle_original"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = DATA_DIR / "output"

# Thu muc luu tai lieu va ket qua validate cua Task 1.
DOCS_DIR = PROJECT_ROOT / "docs"
TASK1_VALIDATION_DIR = OUTPUT_DIR / "task1_validation"
SUPPORTING_TABLES_VALIDATION_DIR = TASK1_VALIDATION_DIR / "supporting_tables"
RELATIONSHIP_VALIDATION_DIR = TASK1_VALIDATION_DIR / "relationship_validation"

# Danh sach tat ca bang raw co trong dataset.
RAW_TABLE_NAMES = [
    "customers",
    "sessions",
    "events",
    "products",
    "orders",
    "order_items",
    "reviews",
]

# Cac bang phu can clean trong Task 1, khong bao gom events vi co script rieng.
TASK1_SUPPORTING_TABLE_NAMES = [
    "customers",
    "sessions",
    "products",
    "orders",
    "order_items",
]
