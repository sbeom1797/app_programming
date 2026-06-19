"""Request and response schemas for the CRM prediction API."""

from pydantic import BaseModel, Field


class CustomerPredictionRequest(BaseModel):
    """Customer-level features used by the classifier and customer regressor."""

    Recency: float = Field(..., ge=0, description="Days since the latest purchase")
    Frequency: float = Field(..., ge=0, description="Number of customer orders")
    Monetary: float = Field(..., ge=0, description="Total customer purchase amount")
    avg_order_amount: float = Field(..., ge=0, description="Average order amount")
    total_quantity: float = Field(..., ge=0, description="Total ordered quantity")
    favorite_category: str = Field("Bikes", description="Favorite product category")
    main_region: str = Field("Australia", description="Main purchase region")


class MarketingPredictionRequest(CustomerPredictionRequest):
    """Customer features plus the campaign cost used for expected profit."""

    marketing_cost: float = Field(5000, ge=0, description="Coupon or campaign cost")


class SalesQuantityPredictionRequest(BaseModel):
    """Product and region features used by the sales quantity regressor."""

    year: int = Field(..., ge=2000, le=2100, description="Prediction year")
    month: int = Field(..., ge=1, le=12, description="Prediction month")
    region: str = Field("Unknown", description="Sales region")
    category: str = Field("Unknown", description="Product category")
    product: str = Field("Unknown", description="Product name")


class ConfusionMatrixInfo(BaseModel):
    """Confusion matrix metadata for the Buy/Not Buy classifier."""

    labels: list[str]
    matrix: list[list[int]]
    image_url: str


class BuyPredictionResponse(BaseModel):
    """Buy or Not Buy classification result."""

    prediction: int
    label: str
    probability: float
    action: str
    confusion_matrix: ConfusionMatrixInfo


class SalesQuantityPredictionResponse(BaseModel):
    """Predicted product sales quantity."""

    predicted_quantity: float
    action: str


class MarketingPredictionResponse(BaseModel):
    """Expected-profit based CRM targeting result."""

    purchase_probability: float
    predicted_sales_amount: float
    marketing_cost: float
    expected_profit: float
    action: str


class PredictionResponse(BaseModel):
    """Backward-compatible generic prediction response."""

    prediction: float | int
    action: str
    probability: float | None = None
