# CRM Marketing Optimization

AdventureWorks Sales 데이터를 사용해 고객 구매 가능성, 예상 구매 금액, 기대이익을 예측하는 CRM 마케팅 최적화 프로젝트입니다.

```text
Expected Profit = Purchase Probability * Predicted Sales Amount - Marketing Cost
```

## 주요 기능

- 고객 구매 여부 예측
- 고객별 기대이익 계산
- 기대이익 기반 마케팅 대상 선정
- 상품/지역별 판매량 예측
- Streamlit 대시보드
- FastAPI 예측 API

## 모델링 기준

과적합과 target leakage를 줄이기 위해 고객 feature와 정답 target을 시간 기준으로 분리합니다.

- 입력 feature: `2020-01-01` 이전 구매 이력으로 만든 RFM/지역/카테고리 정보
- 분류 target: `2020-01-01` 이후 실제 구매 여부
- 회귀 target: `2020-01-01` 이후 실제 구매금액 `Future_Monetary`

따라서 현재 리포트의 성능은 기존의 규칙 기반 라벨/동일 기간 누적 구매금액 예측보다 낮지만, 실제 운영 시나리오에 더 가까운 평가입니다.

## Streamlit Cloud 배포

Streamlit Community Cloud에서 아래 값으로 배포하면 됩니다.

```text
Repository: sbeom1797/app_programming
Branch: main
Main file path: dashboard.py
```

배포된 대시보드는 기본적으로 저장소 안의 모델 파일을 직접 사용합니다. 별도 FastAPI 서버 없이도 `구매 여부 예측`과 `기대이익 분석` 탭이 동작합니다.

단, `models/product_sales_quantity_regressor.pkl`은 GitHub 100MB 제한 때문에 저장소에 포함하지 않았습니다. 그래서 Streamlit Cloud의 `판매량 예측` 탭은 안내 메시지를 표시합니다. 이 탭까지 배포하려면 해당 모델을 Git LFS로 올리거나 FastAPI 서버를 별도로 배포한 뒤 대시보드에서 `FastAPI 서버` 모드를 선택하세요.

## 로컬 실행

패키지 설치:

```powershell
pip install -r requirements.txt
```

Streamlit 실행:

```powershell
python -m streamlit run dashboard.py --server.headless true --browser.gatherUsageStats false
```

접속 주소:

```text
http://127.0.0.1:8501
```

FastAPI 실행:

```powershell
python -m uvicorn app.main:app --reload
```

API 문서:

```text
http://127.0.0.1:8000/docs
```

## 주요 파일

- `dashboard.py`: Streamlit 대시보드
- `app/main.py`: FastAPI 앱
- `app/service.py`: 모델 로딩 및 예측 로직
- `src/preprocessing.py`: 원본 Excel 병합 전처리
- `src/make_features.py`: 고객 단위 피처 생성
- `src/train_classifier.py`: 구매 여부 분류 모델 학습
- `src/train_regressor.py`: 예상 구매 금액 회귀 모델 학습
- `src/train_product_sales_quantity_regressor.py`: 상품 판매량 회귀 모델 학습

## 데이터 준비

원본 Excel 파일은 아래 위치에 둡니다.

```text
data/raw/AdventureWorks Sales (1).xlsx
```

필요 시 전처리부터 다시 실행합니다.

```powershell
python -m src.preprocessing
python -m src.make_features
python -m src.train_regressor
python -m src.train_classifier
```
