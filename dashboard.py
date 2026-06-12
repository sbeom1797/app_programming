"""Streamlit dashboard for the CRM prediction FastAPI service."""

from __future__ import annotations

import os
from typing import Any

import pandas as pd
import requests
import streamlit as st


DEFAULT_API_URL = "http://127.0.0.1:8000"
CUSTOMER_CATEGORIES = ["Accessories", "Bikes", "Clothing"]
FALLBACK_SALES_CATEGORIES = ["Accessories", "Bikes", "Clothing", "Components"]
FALLBACK_PRODUCTS = ["Mountain-200 Black, 38"]


st.set_page_config(
    page_title="CRM 예측 대시보드",
    page_icon="",
    layout="wide",
)


def api_base_url() -> str:
    """Return the FastAPI base URL selected in the sidebar."""
    return st.session_state.get("api_base_url", DEFAULT_API_URL).rstrip("/")


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


def get_static_image_url(image_url: str) -> str:
    """Build an absolute image URL from a relative static path."""
    if image_url.startswith("http"):
        return image_url
    return f"{api_base_url()}{image_url}"


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
        [
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
        ],
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
    payload = customer_payload("buy_")

    if st.button("구매 여부 예측하기", type="primary", key="buy_submit"):
        result = post_prediction("/predict/buy", payload)
        if not result:
            return

        metric_left, metric_middle, metric_right = st.columns(3)
        metric_left.metric("예측 결과", "구매" if result["label"] == "Buy" else "비구매")
        metric_middle.metric("구매 확률", f"{result['probability']:.2%}")
        metric_right.metric("예측 클래스", result["prediction"])
        st.info(translate_action(result["action"]))

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
            image_col.image(
                get_static_image_url(matrix_info["image_url"]),
                caption="분류 모델 혼동 행렬",
                use_container_width=True,
            )


def render_sales_tab() -> None:
    """Render the product-region sales quantity prediction tab."""
    st.subheader("상품 / 지역별 판매량 예측")
    sales_categories, products = load_sales_options()
    left, middle, right = st.columns(3)
    with left:
        year = st.number_input("연도", min_value=2000, max_value=2100, value=2020, step=1)
        region = st.selectbox(
            "지역",
            ["Australia", "Canada", "Central", "France", "Germany", "Northeast", "Northwest", "Southeast", "Southwest", "United Kingdom"],
        )
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

    if st.button("판매량 예측하기", type="primary", key="sales_submit"):
        result = post_prediction("/predict/sales-quantity", payload)
        if not result:
            return
        st.metric("예상 판매량", result["predicted_quantity"])
        st.info(translate_action(result["action"]))


def render_profit_tab() -> None:
    """Render the expected-profit CRM decision tab."""
    st.subheader("기대이익 기반 마케팅 대상 선정")
    payload = customer_payload("profit_")
    marketing_cost = st.number_input(
        "마케팅 비용",
        min_value=0.0,
        value=5000.0,
        step=100.0,
    )
    payload["marketing_cost"] = marketing_cost

    if st.button("기대이익 계산하기", type="primary", key="profit_submit"):
        result = post_prediction("/predict/marketing-profit", payload)
        if not result:
            return

        first, second, third = st.columns(3)
        first.metric("구매 확률", f"{result['purchase_probability']:.2%}")
        second.metric("예상 구매 금액", f"{result['predicted_sales_amount']:,.2f}")
        third.metric("기대이익", f"{result['expected_profit']:,.2f}")
        st.info(translate_action(result["action"]))


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


def main() -> None:
    """Render the Streamlit dashboard."""
    st.title("CRM 예측 대시보드")
    st.caption("FastAPI 모델 결과를 입력 폼과 차트로 확인하는 시연용 대시보드")

    st.sidebar.header("서버 연결")
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
