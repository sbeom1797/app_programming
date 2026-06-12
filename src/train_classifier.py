"""고객별 Buy / Not Buy와 기대수익 기반 마케팅 대상을 선정하는 코드입니다.

핵심 아이디어:
    1. RandomForestClassifier로 고객별 구매확률을 예측합니다.
    2. RandomForestRegressor로 고객별 예상 구매금액을 예측합니다.
    3. 아래 공식으로 고객별 기대수익을 계산합니다.

       Expected_Profit = Purchase_Probability * Predicted_Sales_Amount - Marketing_Cost

    4. 구매확률 순위가 아니라 Expected_Profit이 높은 고객을 최종 마케팅 대상으로 선정합니다.

실행 방법:
    python src/train_classifier.py

주의:
    Expected_Profit 계산에는 rf_regressor.pkl이 필요합니다.
    먼저 python src/train_regressor.py를 실행해 고객별 예상 구매금액 모델을 만들어주세요.
"""

from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "processed" / "customer_features.csv"
MODELS_DIR = PROJECT_ROOT / "models"
FIGURES_DIR = PROJECT_ROOT / "outputs" / "figures"
REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"

MARKETING_COST = 5000
TARGET_COLUMNS = ["Buy_Label", "Buy", "Future_Frequency", "Future_Monetary", "Future_Quantity"]

CLASSIFIER_MODEL_PATH = MODELS_DIR / "rf_classifier.pkl"
REGRESSOR_MODEL_PATH = MODELS_DIR / "rf_regressor.pkl"
CLASSIFIER_FEATURE_COLUMNS_PATH = MODELS_DIR / "classifier_feature_columns.pkl"
REGRESSOR_FEATURE_COLUMNS_PATH = MODELS_DIR / "regressor_feature_columns.pkl"
LEGACY_FEATURE_COLUMNS_PATH = MODELS_DIR / "feature_columns.pkl"

REPORT_PATH = REPORTS_DIR / "classifier_report.txt"
TARGETS_PATH = REPORTS_DIR / "marketing_targets_expected_profit.csv"
ALL_CUSTOMERS_PATH = REPORTS_DIR / "all_customers_expected_profit.csv"
FEATURE_IMPORTANCE_PATH = FIGURES_DIR / "classifier_feature_importance.png"
CONFUSION_MATRIX_PATH = FIGURES_DIR / "confusion_matrix.png"


def load_feature_data() -> pd.DataFrame:
    """고객별 학습 데이터를 불러옵니다."""
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"학습 데이터 파일이 없습니다: {INPUT_PATH}\n"
            "먼저 python src/make_features.py를 실행해서 customer_features.csv를 만들어주세요."
        )

    df = pd.read_csv(INPUT_PATH)
    print(f"Loaded feature data: {df.shape[0]:,} rows, {df.shape[1]:,} columns")
    return df


def prepare_train_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, list[str], str | None]:
    """분류 모델 학습용 feature와 target을 분리합니다."""
    if "Buy_Label" not in df.columns:
        if "Buy" in df.columns:
            df = df.rename(columns={"Buy": "Buy_Label"})
        else:
            raise KeyError(
                "target 컬럼을 찾을 수 없습니다. "
                "customer_features.csv에 Buy_Label 또는 Buy 컬럼이 있어야 합니다."
            )

    customer_id_col = find_customer_id_column(df)

    exclude_columns = [column for column in TARGET_COLUMNS if column in df.columns]
    if customer_id_col:
        exclude_columns.append(customer_id_col)

    feature_columns = [column for column in df.columns if column not in exclude_columns]
    x = df[feature_columns].copy()
    y = df["Buy_Label"].astype(int)

    if y.nunique() < 2:
        raise ValueError("Buy_Label 값이 한 종류뿐이라 분류 모델을 학습할 수 없습니다.")

    print("\n[Train Data]")
    print(f"Feature count: {len(feature_columns):,}")
    print("Target distribution:")
    print(y.value_counts().sort_index())

    return x, y, feature_columns, customer_id_col


def find_customer_id_column(df: pd.DataFrame) -> str | None:
    """고객 식별자 컬럼을 찾습니다."""
    for candidate in ["CustomerKey", "Customer ID", "CustomerID", "Customer"]:
        if candidate in df.columns:
            return candidate
    return None


def train_model(x_train: pd.DataFrame, y_train: pd.Series) -> RandomForestClassifier:
    """RandomForestClassifier 모델을 학습합니다."""
    model = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_split=40,
        min_samples_leaf=20,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(x_train, y_train)
    return model


def evaluate_model(
    model: RandomForestClassifier,
    x_test: pd.DataFrame,
    y_test: pd.Series,
) -> dict[str, object]:
    """Accuracy, Precision, Recall, F1-score, Confusion Matrix를 계산합니다."""
    y_pred = model.predict(x_test)

    return {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred, zero_division=0),
        "recall": recall_score(y_test, y_pred, zero_division=0),
        "f1": f1_score(y_test, y_pred, zero_division=0),
        "confusion_matrix": confusion_matrix(y_test, y_pred),
    }


def save_confusion_matrix_plot(matrix, output_path: Path) -> None:
    """Confusion Matrix 그래프를 저장합니다."""
    plt.figure(figsize=(6, 5))
    sns.heatmap(
        matrix,
        annot=True,
        fmt="d",
        cmap="Blues",
        xticklabels=["Not Buy", "Buy"],
        yticklabels=["Not Buy", "Buy"],
    )
    plt.title("Confusion Matrix")
    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def save_feature_importance(
    model: RandomForestClassifier,
    feature_columns: list[str],
    output_path: Path,
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
    plt.title("Top 15 Feature Importance")
    plt.xlabel("Importance")
    plt.ylabel("Feature")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    return top_15


def load_regressor_model() -> tuple[object, list[str]]:
    """Expected Profit 계산에 필요한 회귀 모델과 feature 컬럼 목록을 불러옵니다."""
    if not REGRESSOR_MODEL_PATH.exists() or not REGRESSOR_FEATURE_COLUMNS_PATH.exists():
        raise FileNotFoundError(
            "Expected_Profit 계산을 위한 회귀 모델 파일이 없습니다.\n"
            "먼저 아래 명령어를 실행하세요.\n\n"
            "python src/train_regressor.py"
        )

    regressor = joblib.load(REGRESSOR_MODEL_PATH)
    regressor_feature_columns = joblib.load(REGRESSOR_FEATURE_COLUMNS_PATH)
    return regressor, regressor_feature_columns


def select_marketing_targets_by_expected_profit(
    classifier: RandomForestClassifier,
    df: pd.DataFrame,
    classifier_feature_columns: list[str],
    customer_id_col: str | None,
    marketing_cost: float = MARKETING_COST,
) -> pd.DataFrame:
    """Expected_Profit이 높은 순서로 최종 마케팅 대상 고객을 선정합니다."""
    working = df.copy()
    if "Buy_Label" not in working.columns and "Buy" in working.columns:
        working = working.rename(columns={"Buy": "Buy_Label"})

    regressor, regressor_feature_columns = load_regressor_model()

    # 1. 분류 모델로 고객별 구매확률을 예측합니다.
    working["Purchase_Probability"] = classifier.predict_proba(
        working[classifier_feature_columns]
    )[:, 1]

    # 2. 회귀 모델로 고객별 예상 구매금액을 예측합니다.
    working["Predicted_Sales_Amount"] = regressor.predict(
        working[regressor_feature_columns]
    )
    working["Predicted_Sales_Amount"] = working["Predicted_Sales_Amount"].clip(lower=0)

    # 3. 기대수익을 계산합니다.
    #    Expected_Profit = Purchase_Probability * Predicted_Sales_Amount - Marketing_Cost
    working["Marketing_Cost"] = marketing_cost
    working["Expected_Profit"] = (
        working["Purchase_Probability"] * working["Predicted_Sales_Amount"]
        - working["Marketing_Cost"]
    )

    # 4. 구매확률이 아니라 Expected_Profit이 높은 순서로 상위 20% 고객을 선정합니다.
    target_count = max(1, int(len(working) * 0.2))
    targets = working.sort_values("Expected_Profit", ascending=False).head(target_count)

    output_columns = [
        "Buy_Label",
        "Purchase_Probability",
        "Predicted_Sales_Amount",
        "Marketing_Cost",
        "Expected_Profit",
    ]
    if customer_id_col:
        output_columns = [customer_id_col] + output_columns

    scored_customers = working[output_columns].sort_values(
        "Expected_Profit",
        ascending=False,
    )
    scored_customers.to_csv(ALL_CUSTOMERS_PATH, index=False, encoding="utf-8-sig")

    return targets[output_columns]


def save_report(
    metrics: dict[str, object],
    top_importance: pd.DataFrame,
    marketing_targets: pd.DataFrame,
) -> None:
    """평가 결과와 Expected Profit 기반 선정 결과를 txt 파일로 저장합니다."""
    matrix = metrics["confusion_matrix"]

    lines = [
        "RandomForestClassifier Evaluation Report",
        "=" * 45,
        f"Accuracy: {metrics['accuracy']:.4f}",
        f"Precision: {metrics['precision']:.4f}",
        f"Recall: {metrics['recall']:.4f}",
        f"F1-score: {metrics['f1']:.4f}",
        "",
        "Confusion Matrix",
        str(matrix),
        "",
        "CRM Marketing Budget Optimization",
        "=" * 45,
        "Expected_Profit = Purchase_Probability * Predicted_Sales_Amount - Marketing_Cost",
        f"Marketing_Cost: {MARKETING_COST:,.0f}",
        f"Marketing target count(top 20% by Expected_Profit): {len(marketing_targets):,}",
        f"Average Expected_Profit among targets: {marketing_targets['Expected_Profit'].mean():,.2f}",
        f"Minimum Expected_Profit among targets: {marketing_targets['Expected_Profit'].min():,.2f}",
        f"Maximum Expected_Profit among targets: {marketing_targets['Expected_Profit'].max():,.2f}",
        "",
        "Top 15 Feature Importance",
        top_importance.to_string(index=False),
    ]

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    """분류 모델 학습과 Expected Profit 기반 대상 선정을 실행합니다."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    df = load_feature_data()
    x, y, feature_columns, customer_id_col = prepare_train_data(df)

    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    classifier = train_model(x_train, y_train)
    metrics = evaluate_model(classifier, x_test, y_test)

    print("\n[Evaluation]")
    print(f"Accuracy: {metrics['accuracy']:.4f}")
    print(f"Precision: {metrics['precision']:.4f}")
    print(f"Recall: {metrics['recall']:.4f}")
    print(f"F1-score: {metrics['f1']:.4f}")
    print("Confusion Matrix:")
    print(metrics["confusion_matrix"])

    save_confusion_matrix_plot(metrics["confusion_matrix"], CONFUSION_MATRIX_PATH)
    top_importance = save_feature_importance(classifier, feature_columns, FEATURE_IMPORTANCE_PATH)

    joblib.dump(classifier, CLASSIFIER_MODEL_PATH)
    joblib.dump(feature_columns, CLASSIFIER_FEATURE_COLUMNS_PATH)
    joblib.dump(feature_columns, LEGACY_FEATURE_COLUMNS_PATH)

    marketing_targets = select_marketing_targets_by_expected_profit(
        classifier,
        df,
        feature_columns,
        customer_id_col,
        marketing_cost=MARKETING_COST,
    )
    marketing_targets.to_csv(TARGETS_PATH, index=False, encoding="utf-8-sig")

    save_report(metrics, top_importance, marketing_targets)

    print("\n[Expected Profit Selection]")
    print(f"Marketing cost: {MARKETING_COST:,.0f}")
    print(f"Target count: {len(marketing_targets):,}")
    print(marketing_targets.head(10).to_string(index=False))

    print("\n[Saved Files]")
    print(f"Classifier model: {CLASSIFIER_MODEL_PATH}")
    print(f"Classifier feature columns: {CLASSIFIER_FEATURE_COLUMNS_PATH}")
    print(f"Report: {REPORT_PATH}")
    print(f"Marketing targets: {TARGETS_PATH}")
    print(f"Feature importance plot: {FEATURE_IMPORTANCE_PATH}")
    print(f"Confusion matrix plot: {CONFUSION_MATRIX_PATH}")


if __name__ == "__main__":
    main()
