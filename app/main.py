"""FastAPI application for CRM marketing and sales predictions."""

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles

from app.schemas import (
    BuyPredictionResponse,
    CustomerPredictionRequest,
    MarketingPredictionRequest,
    MarketingPredictionResponse,
    SalesQuantityPredictionRequest,
    SalesQuantityPredictionResponse,
)
from app.service import (
    predict_buy,
    predict_marketing_profit,
    predict_sales_quantity,
)


app = FastAPI(
    title="CRM Marketing Budget Optimization API",
    description=(
        "Predict customer Buy/Not Buy, product-region sales quantity, "
        "and expected-profit based CRM targeting decisions."
    ),
    version="0.3.0",
)
app.mount("/static", StaticFiles(directory="outputs"), name="static")


@app.get("/")
def health_check():
    """Return a basic API health message."""
    return {"message": "CRM Marketing Budget Optimization API is running"}


@app.post("/predict/buy", response_model=BuyPredictionResponse)
def buy_endpoint(request: CustomerPredictionRequest):
    """Predict whether a customer is likely to buy."""
    try:
        return predict_buy(request)
    except FileNotFoundError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@app.post("/predict/sales-quantity", response_model=SalesQuantityPredictionResponse)
def sales_quantity_endpoint(request: SalesQuantityPredictionRequest):
    """Predict monthly product sales quantity for a region."""
    try:
        return predict_sales_quantity(request)
    except FileNotFoundError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error


@app.post("/predict/sales", response_model=SalesQuantityPredictionResponse)
def sales_endpoint(request: SalesQuantityPredictionRequest):
    """Backward-friendly alias for product sales quantity prediction."""
    return sales_quantity_endpoint(request)


@app.post("/predict/marketing-profit", response_model=MarketingPredictionResponse)
def marketing_profit_endpoint(request: MarketingPredictionRequest):
    """Predict expected profit for a customer-level CRM action."""
    try:
        return predict_marketing_profit(request)
    except FileNotFoundError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
