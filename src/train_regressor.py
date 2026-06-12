"""고객별 예상 구매금액을 예측하는 RandomForestRegressor 학습 코드입니다.

이 프로젝트의 최종 목표는 단순히 구매확률이 높은 고객을 찾는 것이 아니라,
마케팅 비용을 뺀 기대수익(Expected Profit)이 높은 고객을 찾는 것입니다.

이 파일은 그중 두 번째 값인 고객별 예상 구매금액을 예측하는 모델을 만듭니다.

실행 방법:
    python src/train_regressor.py

입력 파일:
    data/processed/customer_features.csv

출력 파일:
    models/rf_regressor.pkl
    models/regressor_feature_columns.pkl
    outputs/reports/regressor_report.txt
    outputs/figures/regressor_feature_importance.png
"""

from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "customer_features.csv"
MODELS_DIR = PROJECT_ROOT / "models"
FIGURES_DIR = PROJECT_ROOT / "outputs" / "figures"
REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"

MODEL_PATH = MODELS_DIR / "rf_regressor.pkl"
FEATURE_COLUMNS_PATH = MODELS_DIR / "regressor_feature_columns.pkl"
REPORT_PATH = REPORTS_DIR / "regressor_report.txt"
FEATURE_IMPORTANCE_PATH = FIGURES_DIR / "regressor_feature_importance.png"


def load_feature_data() -> pd.DataFrame:
    """고객별 feature 데이터를 불러옵니다."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"학습 데이터가 없습니다: {INPUT_PATH}\n"
            "먼저 python src/make_features.py를 실행해서 customer_features.csv를 만들어주세요."
        )

    df = pd.read_csv(INPUT_PATH)
    print(f"Loaded customer feature data: {df.shape[0]:,} rows, {df.shape[1]:,} columns")
    return df


def find_target_column(df: pd.DataFrame) -> str:
    """고객별 예상 구매금액 target 컬럼을 찾습니다.

    고객 단위 마케팅 최적화에서는 한 번의 평균 주문금액보다 고객별 예상 매출 규모가
    더 중요합니다. 따라서 Monetary를 우선 target으로 사용합니다.
    """
    candidates = ["Monetary", "avg_order_amount", "Average_Order_Amount", "AvgOrderAmount"]
    for column in candidates:
        if column in df.columns:
            return column

    raise KeyError(
        "예상 구매금액 target 컬럼을 찾을 수 없습니다. "
        "customer_features.csv에 avg_order_amount 또는 Monetary 컬럼이 필요합니다."
    )


def find_customer_id_column(df: pd.DataFrame) -> str | None:
    """고객 식별자 컬럼을 찾습니다."""
    for candidate in ["CustomerKey", "Customer ID", "CustomerID", "Customer"]:
        if candidate in df.columns:
            return candidate
    return None


def prepare_train_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, list[str], str]:
    """회귀 모델의 feature와 target을 분리합니다."""
    target_col = find_target_column(df)
    customer_id_col = find_customer_id_column(df)

    # target은 고객별 예상 구매금액입니다.
    y = pd.to_numeric(df[target_col], errors="coerce")

    # 고객 ID, 분류 라벨, 회귀 target은 모델 입력에서 제외합니다.
    exclude_columns = [target_col, "Buy", "Buy_Label"]
    if customer_id_col:
        exclude_columns.append(customer_id_col)

    feature_columns = [column for column in df.columns if column not in exclude_columns]
    x = df[feature_columns].copy()

    # 숫자형 변환이 안 되는 값이 있으면 결측 처리 후 제거합니다.
    for column in feature_columns:
        x[column] = pd.to_numeric(x[column], errors="coerce")

    train_data = pd.concat([x, y.rename(target_col)], axis=1).dropna()
    x = train_data[feature_columns]
    y = train_data[target_col]

    print("\n[Train Data]")
    print(f"Target column: {target_col}")
    print(f"Feature count: {len(feature_columns):,}")
    print(f"Training rows: {len(x):,}")

    return x, y, feature_columns, target_col


def train_model(x_train: pd.DataFrame, y_train: pd.Series) -> RandomForestRegressor:
    """RandomForestRegressor 모델을 학습합니다."""
    model = RandomForestRegressor(random_state=42)
    model.fit(x_train, y_train)
    return model


def evaluate_model(
    model: RandomForestRegressor,
    x_test: pd.DataFrame,
    y_test: pd.Series,
) -> dict[str, float]:
    """MAE, RMSE, R2 평가 지표를 계산합니다."""
    predictions = model.predict(x_test)
    mse = mean_squared_error(y_test, predictions)

    return {
        "mae": mean_absolute_error(y_test, predictions),
        "rmse": mse ** 0.5,
        "r2": r2_score(y_test, predictions),
    }


def save_feature_importance(
    model: RandomForestRegressor,
    feature_columns: list[str],
) -> pd.DataFrame:
    """Feature Importance 상위 15개를 출력하고 그래프로 저장합니다."""
    importance_df = pd.DataFrame(
        {
            "feature": feature_columns,
            "importance": model.feature_importances_,
        }
    ).sort_values("importance", ascending=False)

    top_15 = importance_df.head(15)

    print("\n[Top 15 Feature Importance]")
    print(top_15.to_string(index=False))

    plt.figure(figsize=(10, 7))
    sns.barplot(data=top_15, x="importance", y="feature")
    plt.title("Customer Sales Amount Feature Importance")
    plt.xlabel("Importance")
    plt.ylabel("Feature")
    plt.tight_layout()
    plt.savefig(FEATURE_IMPORTANCE_PATH, dpi=150)
    plt.close()

    return top_15


def save_report(
    metrics: dict[str, float],
    target_col: str,
    top_importance: pd.DataFrame,
) -> None:
    """회귀 모델 평가 결과를 txt 파일로 저장합니다."""
    lines = [
        "RandomForestRegressor Evaluation Report",
        "=" * 45,
        "Goal: Predict customer-level expected purchase amount",
        f"Target column: {target_col}",
        "",
        f"MAE: {metrics['mae']:,.4f}",
        f"RMSE: {metrics['rmse']:,.4f}",
        f"R2: {metrics['r2']:.4f}",
        "",
        "Top 15 Feature Importance",
        top_importance.to_string(index=False),
    ]

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    """고객별 예상 구매금액 회귀 모델 학습 전체 과정을 실행합니다."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    df = load_feature_data()
    x, y, feature_columns, target_col = prepare_train_data(df)

    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=0.2,
        random_state=42,
    )

    model = train_model(x_train, y_train)
    metrics = evaluate_model(model, x_test, y_test)

    print("\n[Evaluation]")
    print(f"MAE: {metrics['mae']:,.4f}")
    print(f"RMSE: {metrics['rmse']:,.4f}")
    print(f"R2: {metrics['r2']:.4f}")

    top_importance = save_feature_importance(model, feature_columns)

    joblib.dump(model, MODEL_PATH)
    joblib.dump(feature_columns, FEATURE_COLUMNS_PATH)
    save_report(metrics, target_col, top_importance)

    print("\n[Saved Files]")
    print(f"Model: {MODEL_PATH}")
    print(f"Feature columns: {FEATURE_COLUMNS_PATH}")
    print(f"Report: {REPORT_PATH}")
    print(f"Feature importance plot: {FEATURE_IMPORTANCE_PATH}")


if __name__ == "__main__":
    main()
