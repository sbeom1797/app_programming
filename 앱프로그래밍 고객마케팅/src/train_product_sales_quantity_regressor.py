"""Train a product/region/month sales quantity regression model.

Run:
    python src/train_product_sales_quantity_regressor.py

Input:
    data/processed/merged_sales.csv

Outputs:
    models/product_sales_quantity_regressor.pkl
    models/product_sales_quantity_feature_columns.pkl
    outputs/reports/product_sales_quantity_regressor_report.txt
"""

from pathlib import Path

import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "merged_sales.csv"
MODELS_DIR = PROJECT_ROOT / "models"
REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"

MODEL_PATH = MODELS_DIR / "product_sales_quantity_regressor.pkl"
FEATURE_COLUMNS_PATH = MODELS_DIR / "product_sales_quantity_feature_columns.pkl"
REPORT_PATH = REPORTS_DIR / "product_sales_quantity_regressor_report.txt"


def normalize_column_name(column_name: str) -> str:
    """Normalize column names for tolerant matching across source files."""
    return (
        str(column_name)
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
        .lower()
    )


def find_column(df: pd.DataFrame, candidates: list[str], purpose: str) -> str:
    """Find the first available source column from a list of candidates."""
    normalized_columns = {
        normalize_column_name(column): column
        for column in df.columns
    }

    for candidate in candidates:
        normalized_candidate = normalize_column_name(candidate)
        if normalized_candidate in normalized_columns:
            return normalized_columns[normalized_candidate]

    raise KeyError(
        f"Could not find a column for {purpose}. "
        f"Candidates: {candidates}. Available: {list(df.columns)}"
    )


def load_data() -> pd.DataFrame:
    """Load the merged sales data."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_PATH}. Run src/preprocessing.py first."
        )
    return pd.read_csv(INPUT_PATH)


def build_monthly_product_region_data(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate line-level sales into monthly product-region quantities."""
    date_col = find_column(
        df,
        ["Date", "Full Date", "Order Date", "OrderDate", "OrderDateKey"],
        "order date",
    )
    quantity_col = find_column(
        df,
        ["Order Quantity", "OrderQuantity", "Quantity", "order_quantity"],
        "order quantity",
    )
    region_col = find_column(
        df,
        ["Region", "Country-Region", "Country", "Group"],
        "sales region",
    )
    category_col = find_column(
        df,
        ["Category", "Product Category", "Subcategory"],
        "product category",
    )
    product_col = find_column(
        df,
        ["Product", "Model", "SKU", "ProductKey"],
        "product",
    )

    working = df[[date_col, quantity_col, region_col, category_col, product_col]].copy()
    working[quantity_col] = pd.to_numeric(working[quantity_col], errors="coerce")

    if pd.api.types.is_numeric_dtype(working[date_col]):
        working["_order_date"] = pd.to_datetime(
            working[date_col].astype("Int64").astype(str),
            format="%Y%m%d",
            errors="coerce",
        )
    else:
        working["_order_date"] = pd.to_datetime(working[date_col], errors="coerce")

    working = working.dropna(
        subset=[
            "_order_date",
            quantity_col,
            region_col,
            category_col,
            product_col,
        ]
    )
    working["year"] = working["_order_date"].dt.year
    working["month"] = working["_order_date"].dt.month
    working["region"] = working[region_col].astype(str)
    working["category"] = working[category_col].astype(str)
    working["product"] = working[product_col].astype(str)

    monthly = (
        working.groupby(
            ["year", "month", "region", "category", "product"],
            as_index=False,
        )[quantity_col]
        .sum()
        .rename(columns={quantity_col: "sales_quantity"})
    )
    return monthly


def prepare_train_data(monthly: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, list[str]]:
    """Encode categorical features and split features from the target."""
    encoded = pd.get_dummies(
        monthly,
        columns=["region", "category", "product"],
        prefix=["region", "category", "product"],
        dtype=int,
    )

    y = encoded["sales_quantity"].astype(float)
    x = encoded.drop(columns=["sales_quantity"])
    feature_columns = list(x.columns)
    return x, y, feature_columns


def train_model(x_train: pd.DataFrame, y_train: pd.Series) -> RandomForestRegressor:
    """Train the quantity regression model."""
    model = RandomForestRegressor(
        n_estimators=200,
        random_state=42,
        min_samples_leaf=2,
    )
    model.fit(x_train, y_train)
    return model


def save_report(
    metrics: dict[str, float],
    monthly: pd.DataFrame,
    feature_columns: list[str],
) -> None:
    """Save a compact training report."""
    lines = [
        "Product Sales Quantity Regressor Report",
        "=" * 45,
        "Goal: Predict monthly sales quantity by product/category and region",
        f"Training rows: {len(monthly):,}",
        f"Feature count: {len(feature_columns):,}",
        "",
        f"MAE: {metrics['mae']:,.4f}",
        f"RMSE: {metrics['rmse']:,.4f}",
        f"R2: {metrics['r2']:.4f}",
    ]
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    """Train and save the product sales quantity model."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    df = load_data()
    monthly = build_monthly_product_region_data(df)
    x, y, feature_columns = prepare_train_data(monthly)

    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=0.2,
        random_state=42,
    )

    model = train_model(x_train, y_train)
    predictions = model.predict(x_test)
    mse = mean_squared_error(y_test, predictions)
    metrics = {
        "mae": mean_absolute_error(y_test, predictions),
        "rmse": mse ** 0.5,
        "r2": r2_score(y_test, predictions),
    }

    joblib.dump(model, MODEL_PATH)
    joblib.dump(feature_columns, FEATURE_COLUMNS_PATH)
    save_report(metrics, monthly, feature_columns)

    print("[Evaluation]")
    print(f"Rows: {len(monthly):,}")
    print(f"MAE: {metrics['mae']:,.4f}")
    print(f"RMSE: {metrics['rmse']:,.4f}")
    print(f"R2: {metrics['r2']:.4f}")
    print("\n[Saved Files]")
    print(f"Model: {MODEL_PATH}")
    print(f"Feature columns: {FEATURE_COLUMNS_PATH}")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
