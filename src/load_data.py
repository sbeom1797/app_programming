"""AdventureWorks Sales Excel 파일의 시트 정보를 확인하는 코드입니다.

실행 방법:
    python src/load_data.py
"""

from pathlib import Path

import pandas as pd


# 1. Excel 파일 경로를 변수로 지정합니다.
# Path를 사용하면 Windows/Mac/Linux에서 경로를 비교적 안전하게 다룰 수 있습니다.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXCEL_FILE_PATH = PROJECT_ROOT / "data" / "raw" / "AdventureWorks Sales (1).xlsx"


def print_sheet_names(file_path: Path) -> list[str]:
    """pandas.ExcelFile로 Excel 파일의 전체 시트명을 출력합니다."""
    excel_file = pd.ExcelFile(file_path)
    sheet_names = excel_file.sheet_names

    print("\n[전체 시트명]")
    for index, sheet_name in enumerate(sheet_names, start=1):
        print(f"{index}. {sheet_name}")

    return sheet_names


def load_all_sheets(file_path: Path) -> dict[str, pd.DataFrame]:
    """Excel 파일의 모든 시트를 딕셔너리 형태로 불러옵니다.

    딕셔너리 구조:
        {
            "시트명1": DataFrame,
            "시트명2": DataFrame,
            ...
        }
    """
    sheets = pd.read_excel(file_path, sheet_name=None)
    return sheets


def print_sheet_summary(sheets: dict[str, pd.DataFrame]) -> None:
    """각 시트별 행 개수, 열 개수, 컬럼명을 출력합니다."""
    print("\n[시트별 데이터 요약]")

    for sheet_name, df in sheets.items():
        row_count = df.shape[0]
        column_count = df.shape[1]
        column_names = list(df.columns)

        print("\n" + "=" * 60)
        print(f"시트명: {sheet_name}")
        print(f"행 개수: {row_count:,}")
        print(f"열 개수: {column_count:,}")
        print("컬럼명:")

        for column in column_names:
            print(f"- {column}")


def main() -> None:
    """파일 존재 여부를 확인한 뒤 시트명과 시트별 요약 정보를 출력합니다."""
    print(f"Excel 파일 경로: {EXCEL_FILE_PATH}")

    if not EXCEL_FILE_PATH.exists():
        raise FileNotFoundError(
            f"Excel 파일을 찾을 수 없습니다: {EXCEL_FILE_PATH}\n"
            "파일을 data/raw/AdventureWorks Sales (1).xlsx 위치에 넣은 뒤 다시 실행하세요."
        )

    print_sheet_names(EXCEL_FILE_PATH)
    all_sheets = load_all_sheets(EXCEL_FILE_PATH)
    print_sheet_summary(all_sheets)


# 이 파일을 직접 실행할 때만 main() 함수가 실행됩니다.
# 다른 파일에서 import할 때는 자동 실행되지 않아 재사용하기 좋습니다.
if __name__ == "__main__":
    main()
