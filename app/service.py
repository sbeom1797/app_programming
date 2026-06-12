"""Prediction service functions for saved CRM models."""

from pathlib import Path
import re

import joblib
import pandas as pd

from app.schemas import (
    BuyPredictionResponse,
    ConfusionMatrixInfo,
    CustomerPredictionRequest,
    MarketingPredictionRequest,
    MarketingPredictionResponse,
    SalesQuantityPredictionRequest,
    SalesQuantityPredictionResponse,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = PROJECT_ROOT / "models"
REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
FIGURES_DIR = PROJECT_ROOT / "outputs" / "figures"

CONFUSION_MATRIX_IMAGE_URL = "/static/figures/confusion_matrix.png"
_MODEL_CACHE = {}


def load_model(filename: str):
    """Load a model artifact and refresh it when the saved file changes."""
    path = MODELS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Model artifact not found: {path}. Run the matching training script first."
        )

    modified_at = path.stat().st_mtime
    cached = _MODEL_CACHE.get(filename)
    if cached and cached["modified_at"] == modified_at:
        return cached["artifact"]

    artifact = joblib.load(path)
    _MODEL_CACHE[filename] = {
        "modified_at": modified_at,
        "artifact": artifact,
    }
    return artifact


def build_customer_frame(
    request: CustomerPredictionRequest,
    feature_columns: list[str],
) -> pd.DataFrame:
    """Convert customer API input into the feature layout used at training time."""
    row = {column: 0 for column in feature_columns}

    numeric_values = {
        "Recency": request.Recency,
        "Frequency": request.Frequency,
        "Monetary": request.Monetary,
        "avg_order_amount": request.avg_order_amount,
        "total_quantity": request.total_quantity,
    }
    for column, value in numeric_values.items():
        if column in row:
            row[column] = value

    category_column = f"favorite_category_{request.favorite_category}"
    region_column = f"main_region_{request.main_region}"

    if category_column in row:
        row[category_column] = 1
    if region_column in row:
        row[region_column] = 1

    return pd.DataFrame([row], columns=feature_columns)


def build_sales_quantity_frame(
    request: SalesQuantityPredictionRequest,
    feature_columns: list[str],
) -> pd.DataFrame:
    """Convert product/region API input into the sales model feature layout."""
    raw = pd.DataFrame(
        [
            {
                "year": request.year,
                "month": request.month,
                "region": request.region,
                "category": request.category,
                "product": request.product,
            }
        ]
    )
    encoded = pd.get_dummies(
        raw,
        columns=["region", "category", "product"],
        prefix=["region", "category", "product"],
        dtype=int,
    )
    return encoded.reindex(columns=feature_columns, fill_value=0)


def load_confusion_matrix_info() -> ConfusionMatrixInfo:
    """Load classifier confusion matrix values saved by the training report."""
    report_path = REPORTS_DIR / "classifier_report.txt"
    image_path = FIGURES_DIR / "confusion_matrix.png"
    default_matrix = [[0, 0], [0, 0]]
    image_url = CONFUSION_MATRIX_IMAGE_URL
    if image_path.exists():
        image_url = f"{CONFUSION_MATRIX_IMAGE_URL}?v={int(image_path.stat().st_mtime)}"

    if not report_path.exists():
        return ConfusionMatrixInfo(
            labels=["Not Buy", "Buy"],
            matrix=default_matrix,
            image_url=image_url,
        )

    report_text = report_path.read_text(encoding="utf-8")
    match = re.search(
        r"Confusion Matrix\s*\n\[\[\s*(\d+)\s+(\d+)\]\s*\n\s*\[\s*(\d+)\s+(\d+)\]\]",
        report_text,
    )
    matrix = default_matrix
    if match:
        values = [int(value) for value in match.groups()]
        matrix = [
            [values[0], values[1]],
            [values[2], values[3]],
        ]

    return ConfusionMatrixInfo(
        labels=["Not Buy", "Buy"],
        matrix=matrix,
        image_url=image_url,
    )


def predict_buy(request: CustomerPredictionRequest) -> BuyPredictionResponse:
    """Predict whether a customer is likely to buy."""
    classifier = load_model("rf_classifier.pkl")
    feature_columns = load_model("classifier_feature_columns.pkl")

    frame = build_customer_frame(request, feature_columns)
    buy_probability = float(classifier.predict_proba(frame)[0, 1])
    prediction = int(buy_probability >= 0.5)
    label = "Buy" if prediction == 1 else "Not Buy"
    action = (
        "Target this customer for CRM marketing."
        if prediction == 1
        else "Deprioritize this customer for the current campaign."
    )

    return BuyPredictionResponse(
        prediction=prediction,
        label=label,
        probability=round(buy_probability, 4),
        action=action,
        confusion_matrix=load_confusion_matrix_info(),
    )


def predict_marketing_profit(
    request: MarketingPredictionRequest,
) -> MarketingPredictionResponse:
    """Calculate purchase probability, predicted amount, and expected profit."""
    classifier = load_model("rf_classifier.pkl")
    regressor = load_model("rf_regressor.pkl")
    classifier_features = load_model("classifier_feature_columns.pkl")
    regressor_features = load_model("regressor_feature_columns.pkl")

    classifier_frame = build_customer_frame(request, classifier_features)
    regressor_frame = build_customer_frame(request, regressor_features)

    purchase_probability = float(classifier.predict_proba(classifier_frame)[0, 1])
    predicted_sales_amount = max(0.0, float(regressor.predict(regressor_frame)[0]))
    expected_profit = (
        purchase_probability * predicted_sales_amount
        - request.marketing_cost
    )

    action = (
        "Target this customer because expected profit is positive."
        if expected_profit > 0
        else "Exclude this customer because expected profit is not positive."
    )

    return MarketingPredictionResponse(
        purchase_probability=round(purchase_probability, 4),
        predicted_sales_amount=round(predicted_sales_amount, 2),
        marketing_cost=round(request.marketing_cost, 2),
        expected_profit=round(expected_profit, 2),
        action=action,
    )


def predict_sales_quantity(
    request: SalesQuantityPredictionRequest,
) -> SalesQuantityPredictionResponse:
    """Predict monthly sales quantity for a product/category and region."""
    model = load_model("product_sales_quantity_regressor.pkl")
    feature_columns = load_model("product_sales_quantity_feature_columns.pkl")

    frame = build_sales_quantity_frame(request, feature_columns)
    predicted_quantity = max(0.0, float(model.predict(frame)[0]))
    action = "Use this quantity forecast to plan inventory, campaign budget, and promotions."

    return SalesQuantityPredictionResponse(
        predicted_quantity=round(predicted_quantity, 2),
        action=action,
    )
