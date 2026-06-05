from functools import reduce

from pyspark.sql import functions as F


DEFAULT_LOWERCASE_COLUMNS = [
    "email",
    "country",
    "device",
    "source",
    "payment",
    "payment_method",
    "category",
    "event_type",
]


def clean_text_columns(df, columns, lowercase_columns=None):
    """Chuan hoa cot text bang cach trim khoang trang va lowercase cot can thiet."""
    lowercase_columns = lowercase_columns or DEFAULT_LOWERCASE_COLUMNS

    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))

    for col_name in lowercase_columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.lower(F.col(col_name)))

    return df


def required_fields_condition(required_fields):
    """Tao dieu kien Spark de giu lai cac dong co day du field bat buoc."""
    return reduce(
        lambda condition, col_name: condition & F.col(col_name).isNotNull(),
        required_fields[1:],
        F.col(required_fields[0]).isNotNull(),
    )


def filter_required_fields(df, required_fields):
    """Loc DataFrame theo danh sach field bat buoc khong duoc null."""
    return df.filter(required_fields_condition(required_fields))


def build_null_counts(df):
    """Dem so luong null cua tung cot, dung cho bao cao data quality."""
    return df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ])
