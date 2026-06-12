"""Streamlit dashboard for CRM prediction models.

The dashboard can call a deployed FastAPI service, but it defaults to direct
in-process predictions so it works on Streamlit Community Cloud with only this
repository.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import requests
import streamlit as st

from app.schemas import (
    CustomerPredictionRequest,
    MarketingPredictionRequest,
    SalesQuantityPredictionRequest,
)
from app.service import predict_buy, predict_marketing_profit, predict_sales_quantity


DEFAULT_API_URL = "http://127.0.0.1:8000"
PROJECT_ROOT = Path(__file__).resolve().parent
SALES_MODEL_PATH = PROJECT_ROOT / "models" / "product_sales_quantity_regressor.pkl"
CUSTOMER_CATEGORIES = ["Accessories", "Bikes", "Clothing"]
FALLBACK_SALES_CATEGORIES = ["Accessories", "Bikes", "Clothing", "Components"]
FALLBACK_PRODUCTS = ["Mountain-200 Black, 38"]
ALL_CUSTOMERS_REPORT_PATH = PROJECT_ROOT / "outputs" / "reports" / "all_customers_expected_profit.csv"
MARKETING_TARGETS_REPORT_PATH = PROJECT_ROOT / "outputs" / "reports" / "marketing_targets_expected_profit.csv"
REGIONS = [
    "Australia",
    "Canada",
    "Central",
    "France",
    "Germany",
    "Northeast",
    "Northwest",
    "Southeast",
    "Southwest",
    "United Kingdom",
]


st.set_page_config(
    page_title="CRM 예측 대시보드",
    page_icon="",
    layout="wide",
)


def inject_dashboard_styles() -> None:
    """Apply compact dashboard styling."""
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 3rem;
            padding-bottom: 3rem;
        }
        div[data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            padding: 14px 16px;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        div[data-testid="stMetricLabel"] p {
            color: #64748b;
            font-size: 0.86rem;
        }
        div[data-testid="stMetricValue"] {
            color: #172033;
            font-weight: 750;
        }
        .result-card {
            border: 1px solid #e5e7eb;
            border-left: 5px solid #ff4b4b;
            border-radius: 8px;
            padding: 18px 20px;
            background: #ffffff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
            margin: 10px 0 16px 0;
        }
        .result-card strong {
            display: block;
            color: #172033;
            font-size: 1.02rem;
            margin-bottom: 6px;
        }
        .result-card span {
            color: #64748b;
            font-size: 0.92rem;
        }
        .section-note {
            color: #64748b;
            font-size: 0.92rem;
            margin-top: -0.25rem;
            margin-bottom: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def format_compact_number(value: float) -> str:
    """Format large dashboard numbers compactly."""
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    return f"{value:,.0f}"


@st.cache_data
def load_dashboard_summary() -> dict[str, str]:
    """Load headline metrics from the expected-profit report."""
    if not ALL_CUSTOMERS_REPORT_PATH.exists():
        return {
            "avg_probability": "-",
            "avg_profit": "-",
            "target_count": "-",
            "best_profit": "-",
        }

    df = pd.read_csv(ALL_CUSTOMERS_REPORT_PATH)
    if MARKETING_TARGETS_REPORT_PATH.exists():
        target_count = len(pd.read_csv(MARKETING_TARGETS_REPORT_PATH, usecols=["CustomerKey"]))
    else:
        target_count = int((df["Expected_Profit"] > 0).sum())

    return {
        "avg_probability": f"{df['Purchase_Probability'].mean():.1%}",
        "avg_profit": f"{format_compact_number(df['Expected_Profit'].mean())}",
        "target_count": f"{target_count:,}",
        "best_profit": f"{format_compact_number(df['Expected_Profit'].max())}",
    }


def render_dashboard_summary() -> None:
    """Render top-level model summary metrics."""
    summary = load_dashboard_summary()
    first, second, third, fourth = st.columns(4)
    first.metric("평균 구매확률", summary["avg_probability"])
    second.metric("평균 기대이익", summary["avg_profit"])
    third.metric("상위 후보 수", summary["target_count"])
    fourth.metric("최고 기대이익", summary["best_profit"])


def render_result_card(title: str, detail: str) -> None:
    """Render a compact result callout."""
    st.markdown(
        f"""
        <div class="result-card">
            <strong>{title}</strong>
            <span>{detail}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_probability_gauge(label: str, probability: float) -> None:
    """Render a simple probability gauge."""
    st.caption(label)
    st.progress(max(0.0, min(1.0, probability)))
    left, right = st.columns([1, 1])
    left.caption("0%")
    right.caption("100%")


def render_customer_feature_chart(payload: dict[str, Any]) -> None:
    """Show a small normalized RFM input snapshot."""
    values = pd.Series(
        {
            "Recency": float(payload["Recency"]),
            "Frequency": float(payload["Frequency"]),
            "Monetary": float(payload["Monetary"]),
            "Avg Order": float(payload["avg_order_amount"]),
            "Quantity": float(payload["total_quantity"]),
        }
    )
    normalized = values / values.max() if values.max() else values
    st.caption("입력값 상대 비교")
    st.bar_chart(normalized)


def api_base_url() -> str:
    """Return the FastAPI base URL selected in the sidebar."""
    return st.session_state.get("api_base_url", DEFAULT_API_URL).rstrip("/")


def response_to_dict(response: Any) -> dict[str, Any]:
    """Convert Pydantic v1/v2 models to a plain dictionary."""
    if hasattr(response, "model_dump"):
        return response.model_dump()
    return response.dict()


def call_local_prediction(
    endpoint: str,
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    """Run predictions directly from saved model artifacts."""
    handlers: dict[str, Callable[[Any], Any]] = {
        "/predict/buy": lambda data: predict_buy(CustomerPredictionRequest(**data)),
        "/predict/marketing-profit": lambda data: predict_marketing_profit(
            MarketingPredictionRequest(**data)
        ),
        "/predict/sales-quantity": lambda data: predict_sales_quantity(
            SalesQuantityPredictionRequest(**data)
        ),
    }

    try:
        return response_to_dict(handlers[endpoint](payload))
    except FileNotFoundError as error:
        st.error(str(error))
        return None
    except Exception as error:  # Streamlit should show model issues gracefully.
        st.error(f"예측 중 오류가 발생했습니다: {error}")
        return None


def post_prediction(endpoint: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    """Call a FastAPI prediction endpoint and return parsed JSON."""
    url = f"{api_base_url()}{endpoint}"
    try:
        response = requests.post(url, json=payload, timeout=15)
        response.raise_for_status()
    except requests.RequestException as error:
        st.error(f"API 요청에 실패했습니다: {error}")
        st.caption("FastAPI 서버가 켜져 있는지 확인하세요: python -m uvicorn app.main:app --reload")
        return None
    return response.json()


def predict(endpoint: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    """Dispatch prediction calls according to the selected runtime mode."""
    if st.session_state.get("prediction_mode") == "FastAPI 서버":
        return post_prediction(endpoint, payload)
    return call_local_prediction(endpoint, payload)


def get_static_image_url(image_url: str) -> str:
    """Build an absolute image URL from a relative static path."""
    if image_url.startswith("http"):
        return image_url
    if st.session_state.get("prediction_mode") == "FastAPI 서버":
        return f"{api_base_url()}{image_url}"
    return str(PROJECT_ROOT / image_url.lstrip("/").replace("static/figures", "outputs/figures"))


@st.cache_data
def load_sales_options() -> tuple[list[str], list[str]]:
    """Load category and product options from the processed sales data."""
    try:
        df = pd.read_csv("data/processed/merged_sales.csv", usecols=["Category", "Product"])
    except (FileNotFoundError, ValueError):
        return FALLBACK_SALES_CATEGORIES, FALLBACK_PRODUCTS

    categories = sorted(df["Category"].dropna().astype(str).unique().tolist())
    products = sorted(df["Product"].dropna().astype(str).unique().tolist())
    return categories or FALLBACK_SALES_CATEGORIES, products or FALLBACK_PRODUCTS


def customer_payload(prefix: str = "") -> dict[str, Any]:
    """Render customer feature inputs and return the request payload."""
    left, middle, right = st.columns(3)
    with left:
        recency = st.number_input(
            "최근 구매 후 경과일",
            min_value=0.0,
            value=34.0,
            step=1.0,
            key=f"{prefix}recency",
        )
        avg_order_amount = st.number_input(
            "평균 주문 금액",
            min_value=0.0,
            value=580.35,
            step=10.0,
            key=f"{prefix}avg_order_amount",
        )
    with middle:
        frequency = st.number_input(
            "주문 횟수",
            min_value=0.0,
            value=11.0,
            step=1.0,
            key=f"{prefix}frequency",
        )
        total_quantity = st.number_input(
            "총 주문 수량",
            min_value=0.0,
            value=11.0,
            step=1.0,
            key=f"{prefix}total_quantity",
        )
    with right:
        monetary = st.number_input(
            "총 구매 금액",
            min_value=0.0,
            value=6383.88,
            step=100.0,
            key=f"{prefix}monetary",
        )
        favorite_category = st.selectbox(
            "선호 카테고리",
            CUSTOMER_CATEGORIES,
            index=0,
            key=f"{prefix}favorite_category",
        )

    main_region = st.selectbox(
        "주요 구매 지역",
        REGIONS,
        index=0,
        key=f"{prefix}main_region",
    )

    return {
        "Recency": recency,
        "Frequency": frequency,
        "Monetary": monetary,
        "avg_order_amount": avg_order_amount,
        "total_quantity": total_quantity,
        "favorite_category": favorite_category,
        "main_region": main_region,
    }


def render_buy_tab() -> None:
    """Render the customer Buy/Not Buy prediction tab."""
    st.subheader("고객 구매 여부 예측")
    st.markdown(
        '<p class="section-note">고객의 RFM 정보와 선호 카테고리를 입력해 구매 가능성을 확인합니다.</p>',
        unsafe_allow_html=True,
    )
    payload = customer_payload("buy_")

    if st.button("구매 여부 예측하기", type="primary", key="buy_submit"):
        result = predict("/predict/buy", payload)
        if not result:
            return

        st.divider()
        outcome = "구매 가능성이 높습니다" if result["label"] == "Buy" else "구매 가능성이 낮습니다"
        render_result_card(outcome, translate_action(result["action"]))

        metric_left, metric_middle, metric_right = st.columns(3)
        metric_left.metric("예측 결과", "구매" if result["label"] == "Buy" else "비구매")
        metric_middle.metric("구매 확률", f"{result['probability']:.2%}")
        metric_right.metric("예측 클래스", result["prediction"])

        gauge_col, chart_col = st.columns([1, 1])
        with gauge_col:
            render_probability_gauge("구매 확률 게이지", float(result["probability"]))
        with chart_col:
            render_customer_feature_chart(payload)

        matrix_info = result.get("confusion_matrix")
        if matrix_info:
            st.divider()
            st.subheader("혼동 행렬")
            korean_labels = ["비구매", "구매"]
            matrix_df = pd.DataFrame(
                matrix_info["matrix"],
                index=[f"실제 {label}" for label in korean_labels],
                columns=[f"예측 {label}" for label in korean_labels],
            )
            table_col, image_col = st.columns([1, 1])
            table_col.dataframe(matrix_df, use_container_width=True)
            image_path = get_static_image_url(matrix_info["image_url"])
            if Path(image_path).exists() or image_path.startswith("http"):
                image_col.image(
                    image_path,
                    caption="분류 모델 혼동 행렬",
                    use_container_width=True,
                )


def render_sales_tab() -> None:
    """Render the product-region sales quantity prediction tab."""
    st.subheader("상품 / 지역별 판매량 예측")
    st.markdown(
        '<p class="section-note">월, 지역, 상품 정보를 기준으로 예상 판매량을 계산합니다.</p>',
        unsafe_allow_html=True,
    )
    sales_categories, products = load_sales_options()
    left, middle, right = st.columns(3)
    with left:
        year = st.number_input("연도", min_value=2000, max_value=2100, value=2020, step=1)
        region = st.selectbox("지역", REGIONS)
    with middle:
        month = st.number_input("월", min_value=1, max_value=12, value=12, step=1)
        category = st.selectbox("카테고리", sales_categories)
    with right:
        product = st.selectbox("상품명", products)

    payload = {
        "year": int(year),
        "month": int(month),
        "region": region,
        "category": category,
        "product": product,
    }

    if not SALES_MODEL_PATH.exists() and st.session_state.get("prediction_mode") != "FastAPI 서버":
        st.warning(
            "판매량 예측 모델은 GitHub 100MB 제한 때문에 저장소에 포함하지 않았습니다. "
            "이 탭까지 배포하려면 FastAPI 서버를 연결하거나 Git LFS로 모델을 올려야 합니다."
        )
        return

    if st.button("판매량 예측하기", type="primary", key="sales_submit"):
        result = predict("/predict/sales-quantity", payload)
        if not result:
            return

        st.divider()
        render_result_card(
            "판매량 예측 완료",
            translate_action(result["action"]),
        )
        first, second, third = st.columns(3)
        first.metric("예상 판매량", f"{result['predicted_quantity']:,.2f}")
        second.metric("지역", region)
        third.metric("카테고리", category)


def render_profit_tab() -> None:
    """Render the expected-profit CRM decision tab."""
    st.subheader("기대이익 기반 마케팅 대상 선정")
    st.markdown(
        '<p class="section-note">구매확률과 예상 구매금액에서 마케팅 비용을 차감해 실제 타깃 우선순위를 판단합니다.</p>',
        unsafe_allow_html=True,
    )
    payload = customer_payload("profit_")
    marketing_cost = st.number_input(
        "마케팅 비용",
        min_value=0.0,
        value=5000.0,
        step=100.0,
    )
    payload["marketing_cost"] = marketing_cost

    if st.button("기대이익 계산하기", type="primary", key="profit_submit"):
        result = predict("/predict/marketing-profit", payload)
        if not result:
            return

        st.divider()
        profitable = result["expected_profit"] > 0
        render_result_card(
            "마케팅 대상으로 추천" if profitable else "이번 캠페인에서는 제외 추천",
            translate_action(result["action"]),
        )

        first, second, third = st.columns(3)
        first.metric("구매 확률", f"{result['purchase_probability']:.2%}")
        second.metric("예상 구매 금액", f"{result['predicted_sales_amount']:,.2f}")
        third.metric("기대이익", f"{result['expected_profit']:,.2f}")

        gauge_col, formula_col = st.columns([1, 1])
        with gauge_col:
            render_probability_gauge("구매 확률 게이지", float(result["purchase_probability"]))
        with formula_col:
            formula_df = pd.DataFrame(
                {
                    "항목": ["예상 매출 기여", "마케팅 비용", "기대이익"],
                    "금액": [
                        result["purchase_probability"] * result["predicted_sales_amount"],
                        -result["marketing_cost"],
                        result["expected_profit"],
                    ],
                }
            )
            st.caption("기대이익 구성")
            st.bar_chart(formula_df, x="항목", y="금액")


def translate_action(action: str) -> str:
    """Translate known API action messages for the Korean dashboard."""
    translations = {
        "Target this customer for CRM marketing.": "CRM 마케팅 대상으로 선정하는 것이 좋습니다.",
        "Deprioritize this customer for the current campaign.": "이번 캠페인에서는 우선순위를 낮추는 것이 좋습니다.",
        "Use this quantity forecast to plan inventory, campaign budget, and promotions.": "예상 판매량을 기준으로 재고, 캠페인 예산, 프로모션 강도를 조정하세요.",
        "Target this customer because expected profit is positive.": "기대이익이 양수이므로 마케팅 대상으로 선정하는 것이 좋습니다.",
        "Exclude this customer because expected profit is not positive.": "기대이익이 양수가 아니므로 이번 마케팅 대상에서는 제외하는 것이 좋습니다.",
    }
    return translations.get(action, action)


def render_sidebar() -> None:
    """Render runtime configuration."""
    st.sidebar.header("실행 방식")
    st.sidebar.radio(
        "예측 실행 위치",
        ["내장 모델", "FastAPI 서버"],
        index=0,
        key="prediction_mode",
    )

    if st.session_state.get("prediction_mode") == "FastAPI 서버":
        configured_url = os.getenv("CRM_API_URL", DEFAULT_API_URL)
        st.sidebar.text_input("FastAPI URL", configured_url, key="api_base_url")

        try:
            health = requests.get(f"{api_base_url()}/", timeout=5)
            if health.ok:
                st.sidebar.success("FastAPI 연결됨")
            else:
                st.sidebar.warning("FastAPI 응답 오류")
        except requests.RequestException:
            st.sidebar.error("FastAPI 연결 안 됨")
    else:
        st.sidebar.success("Streamlit에서 모델 직접 실행 중")


def main() -> None:
    """Render the Streamlit dashboard."""
    inject_dashboard_styles()
    st.title("CRM 예측 대시보드")
    st.caption("고객 구매 가능성, 기대이익, 상품 판매량 예측을 확인하는 대시보드")
    render_sidebar()
    render_dashboard_summary()
    st.divider()

    buy_tab, sales_tab, profit_tab = st.tabs(
        ["구매 여부 예측", "판매량 예측", "기대이익 분석"]
    )
    with buy_tab:
        render_buy_tab()
    with sales_tab:
        render_sales_tab()
    with profit_tab:
        render_profit_tab()


if __name__ == "__main__":
    main()
