"""merged_sales.csv를 사용해 EDA 그래프와 RFM 분석 결과를 만드는 코드입니다.

실행 방법:
    python src/eda.py

출력 파일:
    outputs/figures/monthly_sales_trend.png
    outputs/figures/sales_by_region.png
    outputs/figures/sales_by_category.png
    outputs/figures/top_20_customers.png
    outputs/figures/order_quantity_distribution.png
    outputs/figures/rfm_distributions.png
    outputs/reports/rfm_customer_features.csv
"""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "merged_sales.csv"
FIGURES_DIR = PROJECT_ROOT / "outputs" / "figures"
REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"


def find_column(df: pd.DataFrame, candidates: list[str], purpose: str) -> str:
    """후보 컬럼명 중 실제 데이터에 존재하는 첫 번째 컬럼을 찾습니다.

    Excel/CSV 데이터는 공백이 있는 컬럼명과 없는 컬럼명이 섞일 수 있습니다.
    예: "Sales Amount", "SalesAmount", "sales_amount"
    그래서 여러 후보를 넣어두고 자동으로 찾게 만듭니다.
    """
    normalized_columns = {_normalize_column_name(column): column for column in df.columns}

    for candidate in candidates:
        normalized_candidate = _normalize_column_name(candidate)
        if normalized_candidate in normalized_columns:
            return normalized_columns[normalized_candidate]

    raise KeyError(
        f"{purpose}에 사용할 컬럼을 찾을 수 없습니다.\n"
        f"후보 컬럼명: {candidates}\n"
        f"현재 데이터 컬럼명: {list(df.columns)}"
    )


def _normalize_column_name(column_name: str) -> str:
    """컬럼명 비교를 쉽게 하기 위해 공백, 밑줄, 하이픈을 제거하고 소문자로 바꿉니다."""
    return (
        str(column_name)
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
        .lower()
    )


def load_merged_data() -> pd.DataFrame:
    """병합된 CSV 파일을 불러옵니다."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"병합 데이터 파일이 없습니다: {INPUT_PATH}\n"
            "먼저 python src/preprocessing.py를 실행해서 merged_sales.csv를 만들어주세요."
        )

    df = pd.read_csv(INPUT_PATH)
    print(f"Loaded data: {df.shape[0]:,} rows, {df.shape[1]:,} columns")
    return df


def prepare_columns(df: pd.DataFrame) -> dict[str, str]:
    """EDA에 필요한 주요 컬럼을 자동으로 찾습니다."""
    columns = {
        "sales": find_column(df, ["Sales Amount", "SalesAmount", "sales_amount", "Revenue"], "매출 분석"),
        "quantity": find_column(df, ["Order Quantity", "OrderQuantity", "Quantity", "order_quantity"], "주문 수량 분석"),
        "date": find_column(df, ["Date", "Full Date", "Order Date", "OrderDate", "OrderDateKey"], "월별 매출 분석"),
        "region": find_column(df, ["Region", "Country", "Country-Region", "Group"], "지역별 매출 분석"),
        "category": find_column(df, ["Category", "Product Category", "Subcategory"], "상품 카테고리별 매출 분석"),
        "customer": find_column(df, ["CustomerKey", "Customer ID", "Customer", "CustomerID"], "고객별 분석"),
    }

    print("\n[Selected Columns]")
    for purpose, column in columns.items():
        print(f"- {purpose}: {column}")

    return columns


def clean_for_eda(df: pd.DataFrame, columns: dict[str, str]) -> pd.DataFrame:
    """그래프와 RFM 계산에 필요한 컬럼 타입을 정리합니다."""
    result = df.copy()

    result[columns["sales"]] = pd.to_numeric(result[columns["sales"]], errors="coerce")
    result[columns["quantity"]] = pd.to_numeric(result[columns["quantity"]], errors="coerce")
    result["_customer_number"] = pd.to_numeric(result[columns["customer"]], errors="coerce")

    # Date 컬럼이 날짜 문자열이면 그대로 변환하고,
    # OrderDateKey처럼 20170702 형태의 숫자라면 YYYYMMDD 형식으로 변환합니다.
    date_series = result[columns["date"]]
    if pd.api.types.is_numeric_dtype(date_series):
        result["_order_date"] = pd.to_datetime(date_series.astype("Int64").astype(str), format="%Y%m%d", errors="coerce")
    else:
        result["_order_date"] = pd.to_datetime(date_series, errors="coerce")

    result = result.dropna(
        subset=[
            columns["customer"],
            columns["sales"],
            columns["quantity"],
            columns["region"],
            columns["category"],
            "_order_date",
        ]
    )

    before_rows = len(result)
    result = result[result["_customer_number"] > 0].copy()
    removed_rows = before_rows - len(result)
    if removed_rows:
        print(f"Removed non-customer rows(CustomerKey <= 0): {removed_rows:,}")

    result["_year_month"] = result["_order_date"].dt.to_period("M").astype(str)
    result = result.drop(columns=["_customer_number"])
    return result


def save_barplot(data: pd.DataFrame, x: str, y: str, title: str, filename: str) -> None:
    """막대그래프를 저장합니다."""
    plt.figure(figsize=(12, 6))
    sns.barplot(data=data, x=x, y=y)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / filename, dpi=150)
    plt.close()


def plot_monthly_sales(df: pd.DataFrame, sales_col: str) -> None:
    """1. 월별 매출 추세 그래프를 저장합니다."""
    monthly_sales = df.groupby("_year_month", as_index=False)[sales_col].sum()

    plt.figure(figsize=(14, 6))
    sns.lineplot(data=monthly_sales, x="_year_month", y=sales_col, marker="o")
    plt.title("Monthly Sales Trend")
    plt.xlabel("Month")
    plt.ylabel("Sales Amount")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "monthly_sales_trend.png", dpi=150)
    plt.close()


def plot_sales_by_region(df: pd.DataFrame, sales_col: str, region_col: str) -> None:
    """2. 지역별 매출 비교 그래프를 저장합니다."""
    region_sales = (
        df.groupby(region_col, as_index=False)[sales_col]
        .sum()
        .sort_values(sales_col, ascending=False)
    )
    save_barplot(region_sales, sales_col, region_col, "Sales by Region", "sales_by_region.png")


def plot_sales_by_category(df: pd.DataFrame, sales_col: str, category_col: str) -> None:
    """3. 상품 카테고리별 매출 비교 그래프를 저장합니다."""
    category_sales = (
        df.groupby(category_col, as_index=False)[sales_col]
        .sum()
        .sort_values(sales_col, ascending=False)
    )
    save_barplot(category_sales, sales_col, category_col, "Sales by Product Category", "sales_by_category.png")


def plot_top_customers(df: pd.DataFrame, sales_col: str, customer_col: str) -> None:
    """4. 고객별 총 구매금액 상위 20명 그래프를 저장합니다."""
    top_customers = (
        df.groupby(customer_col, as_index=False)[sales_col]
        .sum()
        .sort_values(sales_col, ascending=False)
        .head(20)
    )

    top_customers[customer_col] = top_customers[customer_col].astype(str)
    save_barplot(top_customers, sales_col, customer_col, "Top 20 Customers by Sales", "top_20_customers.png")


def plot_order_quantity_distribution(df: pd.DataFrame, quantity_col: str) -> None:
    """5. 주문 수량 분포 그래프를 저장합니다."""
    plt.figure(figsize=(10, 6))
    sns.histplot(df[quantity_col], bins=50)
    plt.title("Order Quantity Distribution")
    plt.xlabel("Order Quantity")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "order_quantity_distribution.png", dpi=150)
    plt.close()


def calculate_rfm(df: pd.DataFrame, sales_col: str, customer_col: str) -> pd.DataFrame:
    """6. 고객별 Recency, Frequency, Monetary를 계산합니다."""
    analysis_date = df["_order_date"].max() + pd.Timedelta(days=1)

    rfm = (
        df.groupby(customer_col)
        .agg(
            Recency=("_order_date", lambda x: (analysis_date - x.max()).days),
            Frequency=("_order_date", "count"),
            Monetary=(sales_col, "sum"),
        )
        .reset_index()
        .sort_values("Monetary", ascending=False)
    )

    return rfm


def plot_rfm_distributions(rfm: pd.DataFrame) -> None:
    """RFM 세 지표의 분포를 하나의 이미지로 저장합니다."""
    plt.figure(figsize=(15, 4))

    plt.subplot(1, 3, 1)
    sns.histplot(rfm["Recency"], bins=30)
    plt.title("Recency Distribution")

    plt.subplot(1, 3, 2)
    sns.histplot(rfm["Frequency"], bins=30)
    plt.title("Frequency Distribution")

    plt.subplot(1, 3, 3)
    sns.histplot(rfm["Monetary"], bins=30)
    plt.title("Monetary Distribution")

    plt.tight_layout()
    plt.savefig(FIGURES_DIR / "rfm_distributions.png", dpi=150)
    plt.close()


def main() -> None:
    """EDA 전체 흐름을 실행합니다."""
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    df = load_merged_data()
    columns = prepare_columns(df)
    df = clean_for_eda(df, columns)

    plot_monthly_sales(df, columns["sales"])
    plot_sales_by_region(df, columns["sales"], columns["region"])
    plot_sales_by_category(df, columns["sales"], columns["category"])
    plot_top_customers(df, columns["sales"], columns["customer"])
    plot_order_quantity_distribution(df, columns["quantity"])

    rfm = calculate_rfm(df, columns["sales"], columns["customer"])
    rfm_output_path = REPORTS_DIR / "rfm_customer_features.csv"
    rfm.to_csv(rfm_output_path, index=False, encoding="utf-8-sig")
    plot_rfm_distributions(rfm)

    print("\n[EDA Completed]")
    print(f"Figures saved to: {FIGURES_DIR}")
    print(f"RFM report saved to: {rfm_output_path}")


if __name__ == "__main__":
    main()
