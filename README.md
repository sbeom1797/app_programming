# AI 기반 CRM 마케팅 비용 최적화

AdventureWorks Sales 데이터를 이용해 고객별 구매확률과 예상 구매금액을 예측하고, 마케팅 비용을 차감한 기대수익이 높은 고객에게 예산을 집중하는 대학교 기말과제용 프로젝트입니다.

## 프로젝트 목표

모든 고객에게 동일하게 쿠폰이나 광고를 보내면 구매 가능성이 낮거나 예상 구매금액이 작은 고객에게도 비용이 쓰입니다. 이 프로젝트는 RFM 지표와 고객 구매 이력을 기반으로 구매확률과 예상 구매금액을 예측한 뒤, 아래 공식으로 고객별 기대수익을 계산합니다.

```text
Expected_Profit = Purchase_Probability × Predicted_Sales_Amount - Marketing_Cost
```

`Marketing_Cost`는 쿠폰 또는 광고 발송 비용이며 기본값은 5,000으로 가정합니다. 최종 마케팅 대상은 구매확률 순위가 아니라 기대수익이 높은 순서로 선정합니다.

## 데이터 준비

Excel 파일을 아래 위치에 넣어주세요.

```text
data/raw/AdventureWorks Sales (1).xlsx
```

필수 시트:

- `Sales_data`
- `Customer_data`
- `Product_data`
- `Date_data`
- `Sales Territory_data`

## 설치

```powershell
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 모델 학습

프로젝트 루트 폴더에서 실행합니다.

```powershell
python -m src.train_classifier
python -m src.train_regressor
```

학습 후 생성되는 파일:

- `models/rf_classifier.pkl`
- `models/rf_regressor.pkl`
- `models/feature_columns.pkl`
- `data/processed/customer_features.csv`
- `data/processed/sales_features.csv`
- `outputs/figures/*.png`

## FastAPI 실행

```powershell
uvicorn app.main:app --reload
```

브라우저에서 API 문서를 확인할 수 있습니다.

```text
http://127.0.0.1:8000/docs
```

## 주요 API

- `POST /predict/marketing-profit`: 고객 RFM 정보를 입력하면 구매확률, 예상 구매금액, 기대수익, CRM 예산 액션을 반환합니다.
