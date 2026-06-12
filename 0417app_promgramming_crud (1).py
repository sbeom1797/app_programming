import os
import sqlite3
from typing import Optional

import pandas as pd
import gradio as gr
from pydantic import BaseModel, ValidationError, StrictInt, StrictFloat, StrictStr, field_validator


# =========================
# 1) 기본 설정
# =========================
DB_NAME = "customer_users.db"
CSV_FILE = "customers(1).csv"


# =========================
# 2) Pydantic 모델
# =========================
class UserCreate(BaseModel):
    customer_id: StrictStr
    gender: StrictStr
    payment_method: StrictStr
    residence: StrictStr
    membership_grade: StrictStr
    satisfaction: StrictInt
    recent_access_hour: StrictInt
    preferred_temp: StrictFloat
    age: StrictInt
    quantity: StrictInt
    total_payment: StrictInt

    @field_validator("customer_id", "gender", "payment_method", "residence", "membership_grade")
    @classmethod
    def not_empty_string(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("빈 문자열은 허용되지 않습니다.")
        return v

    @field_validator("satisfaction")
    @classmethod
    def validate_satisfaction(cls, v: int) -> int:
        if not (1 <= v <= 5):
            raise ValueError("만족도는 1~5 범위여야 합니다.")
        return v

    @field_validator("recent_access_hour")
    @classmethod
    def validate_hour(cls, v: int) -> int:
        if not (0 <= v <= 23):
            raise ValueError("최근접속시간(시)은 0~23 범위여야 합니다.")
        return v

    @field_validator("age", "quantity", "total_payment")
    @classmethod
    def non_negative_int(cls, v: int) -> int:
        if v < 0:
            raise ValueError("음수는 허용되지 않습니다.")
        return v


class UserUpdate(BaseModel):
    gender: Optional[StrictStr] = None
    payment_method: Optional[StrictStr] = None
    residence: Optional[StrictStr] = None
    membership_grade: Optional[StrictStr] = None
    satisfaction: Optional[StrictInt] = None
    recent_access_hour: Optional[StrictInt] = None
    preferred_temp: Optional[StrictFloat] = None
    age: Optional[StrictInt] = None
    quantity: Optional[StrictInt] = None
    total_payment: Optional[StrictInt] = None

    @field_validator("gender", "payment_method", "residence", "membership_grade")
    @classmethod
    def not_empty_optional_string(cls, v):
        if v is not None and not v.strip():
            raise ValueError("빈 문자열은 허용되지 않습니다.")
        return v

    @field_validator("satisfaction")
    @classmethod
    def validate_satisfaction(cls, v):
        if v is not None and not (1 <= v <= 5):
            raise ValueError("만족도는 1~5 범위여야 합니다.")
        return v

    @field_validator("recent_access_hour")
    @classmethod
    def validate_hour(cls, v):
        if v is not None and not (0 <= v <= 23):
            raise ValueError("최근접속시간(시)은 0~23 범위여야 합니다.")
        return v

    @field_validator("age", "quantity", "total_payment")
    @classmethod
    def non_negative_int(cls, v):
        if v is not None and v < 0:
            raise ValueError("음수는 허용되지 않습니다.")
        return v


# =========================
# 3) 공통 유틸
# =========================
def normalize_int(value):
    if value in (None, ""):
        return None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, int):
        return value
    raise ValueError("정수 필드는 정수만 입력해야 합니다.")


def normalize_float(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    raise ValueError("실수 필드는 숫자만 입력해야 합니다.")


def get_conn():
    return sqlite3.connect(DB_NAME)


def create_table():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            customer_id TEXT PRIMARY KEY,
            gender TEXT NOT NULL,
            payment_method TEXT NOT NULL,
            residence TEXT NOT NULL,
            membership_grade TEXT NOT NULL,
            satisfaction INTEGER NOT NULL,
            recent_access_hour INTEGER NOT NULL,
            preferred_temp REAL NOT NULL,
            age INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            total_payment INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def seed_from_csv():
    if not os.path.exists(CSV_FILE):
        return f"CSV 파일이 없어 초기 적재를 건너뜁니다: {CSV_FILE}"

    df = pd.read_csv(CSV_FILE)
    df = df.rename(columns={
        "고객ID": "customer_id",
        "성별": "gender",
        "결제수단": "payment_method",
        "거주지": "residence",
        "회원등급": "membership_grade",
        "만족도": "satisfaction",
        "최근접속시간(시)": "recent_access_hour",
        "선호제품군_적정온도": "preferred_temp",
        "나이": "age",
        "구매수량": "quantity",
        "총결제금액": "total_payment"
    })

    required_cols = [
        "customer_id", "gender", "payment_method", "residence", "membership_grade",
        "satisfaction", "recent_access_hour", "preferred_temp", "age", "quantity", "total_payment"
    ]
    df = df[required_cols]

    valid_rows = []
    for _, row in df.iterrows():
        try:
            validated = UserCreate.model_validate({
                "customer_id": str(row["customer_id"]),
                "gender": str(row["gender"]),
                "payment_method": str(row["payment_method"]),
                "residence": str(row["residence"]),
                "membership_grade": str(row["membership_grade"]),
                "satisfaction": int(row["satisfaction"]),
                "recent_access_hour": int(row["recent_access_hour"]),
                "preferred_temp": float(row["preferred_temp"]),
                "age": int(row["age"]),
                "quantity": int(row["quantity"]),
                "total_payment": int(row["total_payment"]),
            })
            valid_rows.append(validated)
        except ValidationError:
            continue

    conn = get_conn()
    cur = conn.cursor()
    cur.executemany("""
        INSERT OR REPLACE INTO users (
            customer_id, gender, payment_method, residence, membership_grade,
            satisfaction, recent_access_hour, preferred_temp, age, quantity, total_payment
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (
            u.customer_id, u.gender, u.payment_method, u.residence, u.membership_grade,
            u.satisfaction, u.recent_access_hour, u.preferred_temp, u.age, u.quantity, u.total_payment
        )
        for u in valid_rows
    ])
    conn.commit()
    conn.close()
    return f"초기 적재 완료: {len(valid_rows)}건"


# =========================
# 4) CRUD 함수
# =========================
def create_user(
    customer_id, gender, payment_method, residence, membership_grade,
    satisfaction, recent_access_hour, preferred_temp, age, quantity, total_payment
):
    try:
        user = UserCreate.model_validate({
            "customer_id": customer_id,
            "gender": gender,
            "payment_method": payment_method,
            "residence": residence,
            "membership_grade": membership_grade,
            "satisfaction": normalize_int(satisfaction),
            "recent_access_hour": normalize_int(recent_access_hour),
            "preferred_temp": normalize_float(preferred_temp),
            "age": normalize_int(age),
            "quantity": normalize_int(quantity),
            "total_payment": normalize_int(total_payment)
        })
    except (ValidationError, ValueError) as e:
        raise gr.Error(f"입력 타입 또는 값 오류:\n{e}")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT customer_id FROM users WHERE customer_id = ?", (user.customer_id,))
    if cur.fetchone():
        conn.close()
        raise gr.Error("이미 존재하는 customer_id 입니다.")

    cur.execute("""
        INSERT INTO users (
            customer_id, gender, payment_method, residence, membership_grade,
            satisfaction, recent_access_hour, preferred_temp, age, quantity, total_payment
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        user.customer_id, user.gender, user.payment_method, user.residence, user.membership_grade,
        user.satisfaction, user.recent_access_hour, user.preferred_temp, user.age, user.quantity, user.total_payment
    ))
    conn.commit()
    conn.close()
    return f"생성 완료: {user.customer_id}"


def read_user(customer_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT customer_id, gender, payment_method, residence, membership_grade,
               satisfaction, recent_access_hour, preferred_temp, age, quantity, total_payment
        FROM users
        WHERE customer_id = ?
    """, (customer_id,))
    row = cur.fetchone()
    conn.close()

    columns = [
        "고객ID", "성별", "결제수단", "거주지", "회원등급",
        "만족도", "최근접속시간(시)", "선호제품군_적정온도", "나이", "구매수량", "총결제금액"
    ]

    if not row:
        return pd.DataFrame(columns=columns)

    return pd.DataFrame([row], columns=columns)


def read_all(limit):
    limit = normalize_int(limit) or 10
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT customer_id, gender, payment_method, residence, membership_grade,
               satisfaction, recent_access_hour, preferred_temp, age, quantity, total_payment
        FROM users
        ORDER BY customer_id
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()

    return pd.DataFrame(rows, columns=[
        "고객ID", "성별", "결제수단", "거주지", "회원등급",
        "만족도", "최근접속시간(시)", "선호제품군_적정온도", "나이", "구매수량", "총결제금액"
    ])


def update_user(
    customer_id, gender, payment_method, residence, membership_grade,
    satisfaction, recent_access_hour, preferred_temp, age, quantity, total_payment
):
    try:
        payload = UserUpdate.model_validate({
            "gender": gender or None,
            "payment_method": payment_method or None,
            "residence": residence or None,
            "membership_grade": membership_grade or None,
            "satisfaction": normalize_int(satisfaction),
            "recent_access_hour": normalize_int(recent_access_hour),
            "preferred_temp": normalize_float(preferred_temp),
            "age": normalize_int(age),
            "quantity": normalize_int(quantity),
            "total_payment": normalize_int(total_payment)
        })
    except (ValidationError, ValueError) as e:
        raise gr.Error(f"수정 입력 오류:\n{e}")

    update_data = payload.model_dump(exclude_none=True)
    if not update_data:
        raise gr.Error("수정할 값이 없습니다.")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT customer_id FROM users WHERE customer_id = ?", (customer_id,))
    if not cur.fetchone():
        conn.close()
        raise gr.Error("수정 대상 customer_id가 존재하지 않습니다.")

    set_clause = ", ".join([f"{col} = ?" for col in update_data.keys()])
    values = list(update_data.values()) + [customer_id]

    cur.execute(f"UPDATE users SET {set_clause} WHERE customer_id = ?", values)
    conn.commit()
    conn.close()

    return f"수정 완료: {customer_id}"


def delete_user(customer_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT customer_id FROM users WHERE customer_id = ?", (customer_id,))
    if not cur.fetchone():
        conn.close()
        raise gr.Error("삭제 대상 customer_id가 존재하지 않습니다.")

    cur.execute("DELETE FROM users WHERE customer_id = ?", (customer_id,))
    conn.commit()
    conn.close()
    return f"삭제 완료: {customer_id}"


# =========================
# 5) 앱 시작 시 초기화
# =========================
create_table()
INIT_MSG = seed_from_csv()


# =========================
# 6) 스타일
# =========================
CUSTOM_CSS = """
.gradio-container {
    max-width: 1280px !important;
    margin: 0 auto !important;
    padding-top: 24px !important;
    padding-bottom: 32px !important;
}
.hero-card {
    padding: 24px;
    border-radius: 22px;
    background: linear-gradient(135deg, #e0f2fe 0%, #bfdbfe 100%);
    color: #0f172a;
    box-shadow: 0 12px 30px rgba(0, 0, 0, 0.08);
    margin-bottom: 10px;
}
.hero-card h1 {
    margin: 0 0 8px 0;
    font-size: 30px;
    font-weight: 800;
    color: #0f172a;
}
.hero-card p {
    margin: 0;
    color: #1e293b;
    line-height: 1.6;
}
.section-note {
    padding: 14px 16px;
    border-radius: 14px;
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    margin-bottom: 8px;
}
.guide-card {
    padding: 18px;
    border-radius: 18px;
    background: #f8fafc;
    border: 1px solid #dbeafe;
    box-shadow: 0 6px 16px rgba(15, 23, 42, 0.06);
}
.guide-card h3, .guide-card h4 {
    margin-top: 0.4rem;
    margin-bottom: 0.5rem;
}
.card {
    border-radius: 20px !important;
}
"""


def build_text_inputs(prefix: str):
    return [
        gr.Textbox(label="고객ID", placeholder="예: CUST_001"),
        gr.Textbox(label="gender", placeholder="예: Male / Female"),
        gr.Textbox(label="payment_method", placeholder="예: Card / Cash / Transfer"),
        gr.Textbox(label="거주지", placeholder="예: 서울"),
        gr.Textbox(label="membership_grade", placeholder="예: Gold / Silver"),
        gr.Number(label="만족도", precision=0, minimum=1, maximum=5),
        gr.Number(label="최근접속시간(시)", precision=0, minimum=0, maximum=23),
        gr.Number(label="선호제품군_적정온도", precision=1),
        gr.Number(label="나이", precision=0, minimum=0),
        gr.Number(label="구매수량", precision=0, minimum=0),
        gr.Number(label="총결제금액", precision=0, minimum=0),
    ]




def create_example_data():
    return (
        "CUST_001",
        "여성",
        "카드",
        "서울",
        "Gold",
        5,
        14,
        23.5,
        29,
        3,
        125000
    )


def update_example_data():
    return (
        "CUST_001",
        "남성",
        "계좌이체",
        "경기",
        "Silver",
        4,
        20,
        21.0,
        31,
        5,
        180000
    )


def clear_message():
    return ""


theme = gr.themes.Soft(
    primary_hue="blue",
    secondary_hue="slate",
    neutral_hue="slate",
    radius_size="lg",
)

with gr.Blocks(title="Customer CRUD Service", theme=theme, css=CUSTOM_CSS, fill_width=True) as demo:
    gr.HTML("""
    <div class="hero-card">
        <h1>Customer CRUD Dashboard</h1>
        <p>
            SQLite + Pydantic + Gradio 기반 고객 데이터 관리 서비스입니다.<br/>
            입력값 검증, 단건/전체 조회, 수정, 삭제를 한 화면에서 깔끔하게 처리할 수 있도록 정리했습니다.
        </p>
    </div>
    """)

    with gr.Row():
        with gr.Column(scale=2):
            gr.Markdown("### 서비스 상태")
            gr.Markdown(f"**초기 데이터 로딩 결과:** `{INIT_MSG}`")
        with gr.Column(scale=1):
            gr.Markdown("### 입력 규칙")
            gr.Markdown("- 만족도: 1~5\n- 최근접속시간: 0~23\n- 나이/수량/결제금액: 0 이상")

    with gr.Row():
        with gr.Column(scale=1):
            gr.HTML("""
            <div class="guide-card">
                <h3>📌 데이터 타입 가이드</h3>
                <h4>문자열 (TEXT)</h4>
                <ul>
                    <li><b>고객ID</b>: 예) CUST_001</li>
                    <li><b>성별</b>: 예) 여성 / 남성</li>
                    <li><b>결제수단</b>: 예) 카드 / 현금 / 계좌이체</li>
                    <li><b>거주지</b>: 예) 서울 / 경기</li>
                    <li><b>회원등급</b>: 예) Gold / Silver / Bronze</li>
                </ul>
                <h4>정수 (INTEGER)</h4>
                <ul>
                    <li><b>만족도</b>: 1 ~ 5</li>
                    <li><b>최근접속시간(시)</b>: 0 ~ 23</li>
                    <li><b>나이</b>: 0 이상</li>
                    <li><b>구매수량</b>: 0 이상</li>
                    <li><b>총결제금액</b>: 0 이상</li>
                </ul>
                <h4>실수 (FLOAT)</h4>
                <ul>
                    <li><b>선호제품군_적정온도</b>: 소수 입력 가능 (예: 23.5)</li>
                </ul>
                <h4>⚠️ 입력 주의</h4>
                <ul>
                    <li>빈 문자열 입력 불가</li>
                    <li>정수 칸에 소수 입력 시 오류</li>
                    <li>타입 또는 범위를 벗어나면 즉시 오류 처리</li>
                </ul>
            </div>
            """)
        with gr.Column(scale=3):
            with gr.Tabs():
                with gr.Tab("Create"):
                    gr.HTML('<div class="section-note">새 고객 데이터를 추가합니다. 모든 필드를 정확한 타입으로 입력해야 합니다.</div>')
                    with gr.Group():
                        with gr.Row():
                            with gr.Column():
                                c_customer_id = gr.Textbox(label="고객ID", placeholder="예: CUST_001")
                                c_gender = gr.Textbox(label="성별", placeholder="예: 여성")
                                c_payment_method = gr.Textbox(label="결제수단", placeholder="예: 카드")
                                c_residence = gr.Textbox(label="거주지", placeholder="예: 서울")
                                c_membership_grade = gr.Textbox(label="회원등급", placeholder="예: Gold")
                            with gr.Column():
                                c_satisfaction = gr.Number(label="만족도", precision=0, minimum=1, maximum=5)
                                c_recent_access_hour = gr.Number(label="최근접속시간(시)", precision=0, minimum=0, maximum=23)
                                c_preferred_temp = gr.Number(label="선호제품군_적정온도", precision=1)
                                c_age = gr.Number(label="나이", precision=0, minimum=0)
                                c_quantity = gr.Number(label="구매수량", precision=0, minimum=0)
                                c_total_payment = gr.Number(label="총결제금액", precision=0, minimum=0)

                        with gr.Row():
                            c_btn = gr.Button("생성", variant="primary", scale=1)
                            c_fill_btn = gr.Button("예시 자동 입력", variant="secondary", scale=1)
                            c_clear = gr.ClearButton(
                                components=[
                                    c_customer_id, c_gender, c_payment_method, c_residence, c_membership_grade,
                                    c_satisfaction, c_recent_access_hour, c_preferred_temp, c_age, c_quantity, c_total_payment
                                ],
                                value="초기화",
                                scale=1
                            )
                        c_out = gr.Textbox(label="결과", interactive=False)

                        c_btn.click(
                            fn=create_user,
                            inputs=[
                                c_customer_id, c_gender, c_payment_method, c_residence, c_membership_grade,
                                c_satisfaction, c_recent_access_hour, c_preferred_temp, c_age, c_quantity, c_total_payment
                            ],
                            outputs=c_out
                        )
                        c_fill_btn.click(
                            fn=create_example_data,
                            inputs=[],
                            outputs=[
                                c_customer_id, c_gender, c_payment_method, c_residence, c_membership_grade,
                                c_satisfaction, c_recent_access_hour, c_preferred_temp, c_age, c_quantity, c_total_payment
                            ]
                        )

                with gr.Tab("Read"):
                    gr.HTML('<div class="section-note">고객ID로 단건 조회하거나, 조회건수를 지정해 전체 목록을 확인할 수 있습니다.</div>')
                    with gr.Row():
                        with gr.Column():
                            gr.Markdown("#### 단건 조회")
                            with gr.Row():
                                r_customer_id = gr.Textbox(label="고객ID", placeholder="조회할 고객 ID")
                                r_btn = gr.Button("단건 조회", variant="primary")
                            r_out = gr.Dataframe(label="조회 결과", interactive=False, wrap=True)

                        with gr.Column():
                            gr.Markdown("#### 전체 조회")
                            with gr.Row():
                                r_limit = gr.Number(value=10, label="조회건수", precision=0, minimum=1)
                                r_all_btn = gr.Button("전체 조회")
                            r_all_out = gr.Dataframe(label="전체 결과", interactive=False, wrap=True)

                    r_btn.click(fn=read_user, inputs=r_customer_id, outputs=r_out)
                    r_all_btn.click(fn=read_all, inputs=r_limit, outputs=r_all_out)

                with gr.Tab("Update"):
                    gr.HTML('<div class="section-note">수정할 필드만 입력하면 됩니다. 비워둔 값은 그대로 유지됩니다.</div>')
                    with gr.Group():
                        with gr.Row():
                            with gr.Column():
                                u_customer_id = gr.Textbox(label="고객ID", placeholder="수정할 고객 ID")
                                u_gender = gr.Textbox(label="성별", placeholder="변경 시 입력")
                                u_payment_method = gr.Textbox(label="결제수단", placeholder="변경 시 입력")
                                u_residence = gr.Textbox(label="거주지", placeholder="변경 시 입력")
                                u_membership_grade = gr.Textbox(label="회원등급", placeholder="변경 시 입력")
                            with gr.Column():
                                u_satisfaction = gr.Number(label="만족도", precision=0, minimum=1, maximum=5)
                                u_recent_access_hour = gr.Number(label="최근접속시간(시)", precision=0, minimum=0, maximum=23)
                                u_preferred_temp = gr.Number(label="선호제품군_적정온도", precision=1)
                                u_age = gr.Number(label="나이", precision=0, minimum=0)
                                u_quantity = gr.Number(label="구매수량", precision=0, minimum=0)
                                u_total_payment = gr.Number(label="총결제금액", precision=0, minimum=0)

                        with gr.Row():
                            u_btn = gr.Button("수정", variant="primary", scale=1)
                            u_fill_btn = gr.Button("예시 자동 입력", variant="secondary", scale=1)
                            u_clear = gr.ClearButton(
                                components=[
                                    u_customer_id, u_gender, u_payment_method, u_residence, u_membership_grade,
                                    u_satisfaction, u_recent_access_hour, u_preferred_temp, u_age, u_quantity, u_total_payment
                                ],
                                value="초기화",
                                scale=1
                            )
                        u_out = gr.Textbox(label="결과", interactive=False)

                        u_btn.click(
                            fn=update_user,
                            inputs=[
                                u_customer_id, u_gender, u_payment_method, u_residence, u_membership_grade,
                                u_satisfaction, u_recent_access_hour, u_preferred_temp, u_age, u_quantity, u_total_payment
                            ],
                            outputs=u_out
                        )
                        u_fill_btn.click(
                            fn=update_example_data,
                            inputs=[],
                            outputs=[
                                u_customer_id, u_gender, u_payment_method, u_residence, u_membership_grade,
                                u_satisfaction, u_recent_access_hour, u_preferred_temp, u_age, u_quantity, u_total_payment
                            ]
                        )

                with gr.Tab("Delete"):
                    gr.HTML('<div class="section-note">고객ID 기준으로 데이터를 삭제합니다. 삭제 후 복구되지 않습니다.</div>')
                    with gr.Row():
                        d_customer_id = gr.Textbox(label="고객ID", placeholder="삭제할 고객 ID")
                        d_btn = gr.Button("삭제", variant="stop")
                    d_out = gr.Textbox(label="결과", interactive=False)
                    d_btn.click(fn=delete_user, inputs=d_customer_id, outputs=d_out)


if __name__ == "__main__":
    demo.launch(share=True)
