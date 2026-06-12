"""RandomForestClassifier 학습용 고객별 Feature 데이터셋을 생성합니다.

실행 방법:
    python src/make_features.py

입력 파일:
    data/processed/merged_sales.csv

출력 파일:
    data/processed/customer_features.csv
    models/feature_columns.pkl
"""

from pathlib import Path

import joblib
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "merged_sales.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "customer_features.csv"
MODELS_DIR = PROJECT_ROOT / "models"
FEATURE_COLUMNS_PATH = MODELS_DIR / "feature_columns.pkl"


def find_column(df: pd.DataFrame, candidates: list[str], purpose: str) -> str:
    """후보 컬럼명 중 실제 데이터에 존재하는 컬럼을 찾습니다.

    데이터셋마다 "Sales Amount"처럼 공백이 있을 수도 있고,
    "SalesAmount"처럼 공백이 없을 수도 있습니다.
    그래서 비교할 때는 공백, 밑줄, 하이픈을 제거하고 소문자로 바꿔 비교합니다.
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
    """컬럼명을 비교하기 쉽게 정규화합니다."""
    return (
        str(column_name)
        .replace(" ", "")
        .replace("_", "")
        .replace("-", "")
        .lower()
    )


def load_data() -> pd.DataFrame:
    """병합된 매출 데이터를 불러옵니다."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"입력 파일이 없습니다: {INPUT_PATH}\n"
            "먼저 python src/preprocessing.py를 실행해서 merged_sales.csv를 만들어주세요."
        )

    df = pd.read_csv(INPUT_PATH)
    print(f"Loaded merged data: {df.shape[0]:,} rows, {df.shape[1]:,} columns")
    return df


def prepare_base_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, str]]:
    """RFM과 추가 변수 생성에 필요한 컬럼을 찾고 타입을 정리합니다."""
    columns = {
        "customer": find_column(df, ["CustomerKey", "Customer ID", "CustomerID", "Customer"], "고객 식별"),
        "order": find_column(df, ["SalesOrderLineKey", "Sales Order", "OrderID", "Order Number"], "주문 횟수 계산"),
        "date": find_column(df, ["Date", "Full Date", "Order Date", "OrderDate", "OrderDateKey"], "최근 구매일 계산"),
        "sales": find_column(df, ["Sales Amount", "SalesAmount", "Revenue", "sales_amount"], "총 구매금액 계산"),
        "quantity": find_column(df, ["Order Quantity", "OrderQuantity", "Quantity", "order_quantity"], "총 주문수량 계산"),
        "category": find_column(df, ["Category", "Product Category", "Subcategory"], "선호 상품 카테고리 계산"),
        "region": find_column(df, ["Region", "Country", "Country-Region", "Group"], "주요 구매 지역 계산"),
    }

    print("\n[Selected Columns]")
    for name, column in columns.items():
        print(f"- {name}: {column}")

    cleaned = df.copy()
    cleaned[columns["sales"]] = pd.to_numeric(cleaned[columns["sales"]], errors="coerce")
    cleaned[columns["quantity"]] = pd.to_numeric(cleaned[columns["quantity"]], errors="coerce")

    # 날짜 컬럼이 20170702 같은 숫자라면 YYYYMMDD로 해석하고,
    # "2017-07-02" 같은 문자열이면 pandas가 자동으로 날짜로 변환하게 합니다.
    date_series = cleaned[columns["date"]]
    if pd.api.types.is_numeric_dtype(date_series):
        cleaned["_order_date"] = pd.to_datetime(
            date_series.astype("Int64").astype(str),
            format="%Y%m%d",
            errors="coerce",
        )
    else:
        cleaned["_order_date"] = pd.to_datetime(date_series, errors="coerce")

    # 핵심 컬럼이 비어 있으면 학습 데이터로 사용할 수 없으므로 제거합니다.
    cleaned = cleaned.dropna(
        subset=[
            columns["customer"],
            columns["order"],
            columns["sales"],
            columns["quantity"],
            columns["category"],
            columns["region"],
            "_order_date",
        ]
    )

    # AdventureWorks 데이터에는 CustomerKey가 -1인 "[Not Applicable]" 고객이 있습니다.
    # 이 값은 실제 CRM 쿠폰/광고를 보낼 수 있는 개인 고객이 아니므로 분석 대상에서 제외합니다.
    customer_as_number = pd.to_numeric(cleaned[columns["customer"]], errors="coerce")
    if customer_as_number.notna().any():
        before_rows = len(cleaned)
        cleaned = cleaned[customer_as_number > 0].copy()
        removed_rows = before_rows - len(cleaned)
        print(f"Removed non-customer rows(CustomerKey <= 0): {removed_rows:,}")

    return cleaned, columns


def most_frequent_value(series: pd.Series) -> str:
    """고객별 선호 카테고리/주요 지역을 구하기 위한 최빈값 함수입니다."""
    mode = series.dropna().mode()
    if mode.empty:
        return "Unknown"
    return str(mode.iloc[0])


def make_customer_features(df: pd.DataFrame, columns: dict[str, str]) -> pd.DataFrame:
    """고객별 RFM 지표와 추가 변수를 생성합니다."""
    customer_col = columns["customer"]
    order_col = columns["order"]
    sales_col = columns["sales"]
    quantity_col = columns["quantity"]
    category_col = columns["category"]
    region_col = columns["region"]

    # 전체 데이터의 마지막 구매일을 기준일로 사용합니다.
    # 예를 들어 전체 마지막 구매일이 2020-12-31이고 고객의 마지막 구매일이 2020-12-01이면
    # Recency는 30일입니다.
    last_date = df["_order_date"].max()

    customer_features = (
        df.groupby(customer_col)
        .agg(
            last_purchase_date=("_order_date", "max"),
            Frequency=(order_col, "nunique"),
            Monetary=(sales_col, "sum"),
            total_quantity=(quantity_col, "sum"),
            favorite_category=(category_col, most_frequent_value),
            main_region=(region_col, most_frequent_value),
        )
        .reset_index()
    )

    customer_features["Recency"] = (
        last_date - customer_features["last_purchase_date"]
    ).dt.days

    # 평균 주문금액 = 총 구매금액 / 주문 횟수
    customer_features["avg_order_amount"] = (
        customer_features["Monetary"] / customer_features["Frequency"].clip(lower=1)
    )

    customer_features = add_buy_label(customer_features)
    customer_features = customer_features.drop(columns=["last_purchase_date"])
    return customer_features


def add_buy_label(customer_features: pd.DataFrame) -> pd.DataFrame:
    """Buy/Not Buy 라벨을 생성합니다.

    라벨 기준:
    - Buy = 1:
      1. 고객의 Recency가 전체 고객 Recency의 중앙값보다 작거나 같은 고객
         -> 최근에 구매한 고객을 의미합니다.
      2. 고객의 Frequency가 전체 고객 Frequency의 중앙값보다 크거나 같은 고객
         -> 자주 구매한 고객을 의미합니다.
      3. 위 두 조건을 모두 만족하는 고객
         -> 최근에 자주 구매했으므로 CRM 마케팅 반응 가능성이 높은 고객으로 봅니다.

    - Buy = 0:
      위 조건에 해당하지 않는 고객

    이 기준은 최근에 자주 구매한 고객을 마케팅 반응 가능성이 높은 고객으로 보는
    CRM 관점의 실습용 라벨입니다.
    """
    recency_median = customer_features["Recency"].median()
    frequency_median = customer_features["Frequency"].median()

    customer_features["Buy"] = (
        (customer_features["Recency"] <= recency_median)
        & (customer_features["Frequency"] >= frequency_median)
    ).astype(int)

    print("\n[Buy Label Summary]")
    print(customer_features["Buy"].value_counts().sort_index())
    print(f"Recency median: {recency_median:.2f}")
    print(f"Frequency median: {frequency_median:.2f}")

    return customer_features


def handle_missing_values(customer_features: pd.DataFrame) -> pd.DataFrame:
    """결측치를 처리합니다."""
    result = customer_features.copy()

    numeric_columns = ["Recency", "Frequency", "Monetary", "avg_order_amount", "total_quantity"]
    categorical_columns = ["favorite_category", "main_region"]

    for column in numeric_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce")
        result[column] = result[column].fillna(0)

    for column in categorical_columns:
        result[column] = result[column].fillna("Unknown").astype(str)

    return result


def encode_categorical_features(customer_features: pd.DataFrame) -> pd.DataFrame:
    """범주형 변수에 One-Hot Encoding을 적용합니다."""
    categorical_columns = ["favorite_category", "main_region"]

    encoded = pd.get_dummies(
        customer_features,
        columns=categorical_columns,
        prefix=categorical_columns,
        dtype=int,
    )
    return encoded


def save_feature_data(feature_data: pd.DataFrame) -> None:
    """최종 학습 데이터와 feature 컬럼 목록을 저장합니다."""
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    # 모델 학습 feature는 고객 식별자와 정답 라벨 Buy를 제외한 모든 컬럼입니다.
    feature_columns = [
        column for column in feature_data.columns if column not in ["CustomerKey", "Customer ID", "Customer", "Buy"]
    ]

    feature_data.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    joblib.dump(feature_columns, FEATURE_COLUMNS_PATH)

    print("\n[Saved Files]")
    print(f"Customer feature data: {OUTPUT_PATH}")
    print(f"Feature columns: {FEATURE_COLUMNS_PATH}")
    print(f"Feature count: {len(feature_columns):,}")


def main() -> None:
    """Feature 생성 전체 과정을 실행합니다."""
    df = load_data()
    df, columns = prepare_base_columns(df)
    customer_features = make_customer_features(df, columns)
    customer_features = handle_missing_values(customer_features)
    customer_features = encode_categorical_features(customer_features)
    save_feature_data(customer_features)

    print("\n[Preview]")
    print(customer_features.head())


if __name__ == "__main__":
    main()
