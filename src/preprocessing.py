"""AdventureWorks Sales 주요 시트를 병합하는 전처리 코드입니다.

실행 방법:
    python src/preprocessing.py

이 파일은 다음 작업을 합니다.
1. Excel 파일에서 주요 시트를 불러옵니다.
2. 각 시트의 컬럼명을 출력합니다.
3. Sales_data를 중심으로 Customer, Product, Date, Territory 데이터를 병합합니다.
4. 병합 후 결측치와 중복 행 개수를 확인합니다.
5. 중복 행을 제거한 뒤 data/processed/merged_sales.csv로 저장합니다.

참고:
    .xlsx 파일을 읽으려면 openpyxl 패키지가 필요합니다.
    기본 Python에 openpyxl이 없고 프로젝트 venv가 있으면, 이 스크립트가 자동으로
    .venv가 아닌 현재 프로젝트의 venv Python으로 다시 실행됩니다.
"""

from pathlib import Path
import subprocess
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXCEL_FILE_PATH = PROJECT_ROOT / "data" / "raw" / "AdventureWorks Sales (1).xlsx"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
MERGED_OUTPUT_PATH = PROCESSED_DIR / "merged_sales.csv"

REQUIRED_SHEETS = [
    "Sales_data",
    "Customer_data",
    "Product_data",
    "Date_data",
    "Sales Territory_data",
]


def ensure_excel_engine() -> None:
    """Excel 읽기에 필요한 openpyxl이 없으면 프로젝트 venv Python으로 재실행합니다."""
    try:
        import openpyxl  # noqa: F401
        return
    except ImportError:
        venv_python = PROJECT_ROOT / "venv" / "Scripts" / "python.exe"

        # 사용자가 기본 Python으로 실행했더라도 프로젝트 venv가 있으면 자동으로 다시 실행합니다.
        if venv_python.exists() and Path(sys.executable).resolve() != venv_python.resolve():
            print("[안내] 현재 Python에 openpyxl이 없어 프로젝트 venv Python으로 다시 실행합니다.", flush=True)
            print(f"[안내] 재실행 Python: {venv_python}", flush=True)
            completed = subprocess.run([str(venv_python), str(Path(__file__).resolve())])
            raise SystemExit(completed.returncode)

        raise ImportError(
            "Excel 파일(.xlsx)을 읽기 위한 openpyxl 패키지가 없습니다.\n"
            "아래 명령어로 패키지를 설치한 뒤 다시 실행하세요.\n\n"
            "pip install openpyxl\n\n"
            "또는 프로젝트 가상환경을 사용하세요.\n"
            ".\\venv\\Scripts\\python.exe src\\preprocessing.py"
        )


def load_required_sheets(file_path: Path) -> dict[str, pd.DataFrame]:
    """Excel 파일에서 주요 시트만 불러와 딕셔너리로 반환합니다."""
    if not file_path.exists():
        raise FileNotFoundError(
            f"Excel 파일을 찾을 수 없습니다: {file_path}\n"
            "data/raw/AdventureWorks Sales (1).xlsx 위치에 파일을 넣어주세요."
        )

    excel_file = pd.ExcelFile(file_path)
    available_sheets = excel_file.sheet_names

    print("[Excel 전체 시트명]")
    for sheet_name in available_sheets:
        print(f"- {sheet_name}")

    sheets = {}
    for sheet_name in REQUIRED_SHEETS:
        if sheet_name not in available_sheets:
            raise ValueError(
                f"필수 시트가 없습니다: {sheet_name}\n"
                f"현재 Excel에 있는 시트명: {available_sheets}"
            )
        sheets[sheet_name] = pd.read_excel(file_path, sheet_name=sheet_name)

    return sheets


def print_columns_by_sheet(sheets: dict[str, pd.DataFrame]) -> None:
    """각 시트의 컬럼명을 출력해서 병합 키를 쉽게 확인할 수 있게 합니다."""
    print("\n[시트별 컬럼명]")
    for sheet_name, df in sheets.items():
        print("\n" + "=" * 70)
        print(f"시트명: {sheet_name}")
        print(f"행/열 개수: {df.shape[0]:,}행, {df.shape[1]:,}열")
        print("컬럼명:")
        for column in df.columns:
            print(f"- {column}")


def check_columns(df: pd.DataFrame, required_columns: list[str], table_name: str) -> bool:
    """DataFrame에 필요한 컬럼이 있는지 확인합니다."""
    missing_columns = [column for column in required_columns if column not in df.columns]

    if missing_columns:
        print(f"\n[경고] {table_name}에 필요한 컬럼이 없습니다.")
        print(f"없는 컬럼: {missing_columns}")
        print(f"현재 컬럼: {list(df.columns)}")
        return False

    return True


def merge_if_key_exists(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_key: str,
    right_key: str,
    right_table_name: str,
) -> pd.DataFrame:
    """공통 키가 있으면 left join으로 병합하고, 없으면 친절한 메시지를 출력합니다."""
    left_has_key = left_key in left.columns
    right_has_key = right_key in right.columns

    if not left_has_key or not right_has_key:
        print(f"\n[경고] {right_table_name} 병합을 건너뜁니다.")
        if not left_has_key:
            print(f"- Sales_data에 '{left_key}' 컬럼이 없습니다.")
        if not right_has_key:
            print(f"- {right_table_name}에 '{right_key}' 컬럼이 없습니다.")
        return left

    before_rows = len(left)
    merged = left.merge(
        right,
        how="left",
        left_on=left_key,
        right_on=right_key,
        suffixes=("", f"_{right_table_name}"),
    )
    after_rows = len(merged)

    print(f"\n[병합 완료] {right_table_name}")
    print(f"- 병합 키: Sales_data.{left_key} = {right_table_name}.{right_key}")
    print(f"- 병합 전 행 개수: {before_rows:,}")
    print(f"- 병합 후 행 개수: {after_rows:,}")

    return merged


def merge_main_sheets(sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Sales_data를 중심으로 주요 시트를 병합합니다."""
    sales = sheets["Sales_data"].copy()
    customer = sheets["Customer_data"].copy()
    product = sheets["Product_data"].copy()
    date = sheets["Date_data"].copy()
    territory = sheets["Sales Territory_data"].copy()

    check_columns(sales, ["CustomerKey", "ProductKey", "OrderDateKey", "SalesTerritoryKey"], "Sales_data")
    check_columns(customer, ["CustomerKey"], "Customer_data")
    check_columns(product, ["ProductKey"], "Product_data")
    check_columns(date, ["DateKey"], "Date_data")
    check_columns(territory, ["SalesTerritoryKey"], "Sales Territory_data")

    merged = sales
    merged = merge_if_key_exists(merged, customer, "CustomerKey", "CustomerKey", "Customer_data")
    merged = merge_if_key_exists(merged, product, "ProductKey", "ProductKey", "Product_data")
    merged = merge_if_key_exists(merged, date, "OrderDateKey", "DateKey", "Date_data")
    merged = merge_if_key_exists(
        merged,
        territory,
        "SalesTerritoryKey",
        "SalesTerritoryKey",
        "Sales Territory_data",
    )

    return merged


def print_quality_report(df: pd.DataFrame, title: str) -> None:
    """결측치 개수와 중복 행 개수를 출력합니다."""
    print("\n" + "=" * 70)
    print(f"[{title}]")
    print(f"행 개수: {len(df):,}")
    print(f"열 개수: {df.shape[1]:,}")
    print(f"전체 결측치 개수: {df.isna().sum().sum():,}")
    print(f"중복 행 개수: {df.duplicated().sum():,}")

    print("\n컬럼별 결측치 개수 상위 10개:")
    missing_top_10 = df.isna().sum().sort_values(ascending=False).head(10)
    print(missing_top_10)


def save_merged_data(df: pd.DataFrame, output_path: Path) -> None:
    """병합된 데이터를 CSV 파일로 저장합니다."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n[저장 완료] {output_path}")


def main() -> None:
    """전처리 전체 흐름을 실행합니다."""
    ensure_excel_engine()
    print(f"Excel 파일 경로: {EXCEL_FILE_PATH}")

    sheets = load_required_sheets(EXCEL_FILE_PATH)
    print_columns_by_sheet(sheets)

    merged = merge_main_sheets(sheets)
    print_quality_report(merged, "중복 제거 전 병합 데이터 품질 확인")

    merged_without_duplicates = merged.drop_duplicates()
    print_quality_report(merged_without_duplicates, "중복 제거 후 병합 데이터 품질 확인")

    save_merged_data(merged_without_duplicates, MERGED_OUTPUT_PATH)


if __name__ == "__main__":
    main()
