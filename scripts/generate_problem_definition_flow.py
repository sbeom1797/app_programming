from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch


OUTPUT_PATH = Path("outputs/figures/problem_definition_flow.png")


def add_box(ax, x, y, w, h, text, facecolor, edgecolor="#2f3a4a", fontsize=13):
    box = FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle="round,pad=0.02,rounding_size=0.035",
        linewidth=1.6,
        edgecolor=edgecolor,
        facecolor=facecolor,
    )
    ax.add_patch(box)
    ax.text(
        x + w / 2,
        y + h / 2,
        text,
        ha="center",
        va="center",
        fontsize=fontsize,
        color="#172033",
        linespacing=1.35,
    )


def add_arrow(ax, start, end, color="#516173"):
    arrow = FancyArrowPatch(
        start,
        end,
        arrowstyle="-|>",
        mutation_scale=20,
        linewidth=1.8,
        color=color,
        shrinkA=6,
        shrinkB=6,
    )
    ax.add_patch(arrow)


def main():
    plt.rcParams["font.family"] = ["Malgun Gothic", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False

    fig, ax = plt.subplots(figsize=(14, 8), dpi=160)
    fig.patch.set_facecolor("#f7f8fb")
    ax.set_facecolor("#f7f8fb")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    ax.text(
        0.5,
        0.93,
        "CRM 마케팅 예산 최적화 문제정의",
        ha="center",
        va="center",
        fontsize=24,
        fontweight="bold",
        color="#152033",
    )
    ax.text(
        0.5,
        0.875,
        "구매확률과 예상매출을 결합해, 비용 대비 기대이익이 높은 고객을 우선 선정",
        ha="center",
        va="center",
        fontsize=14,
        color="#4d5a6c",
    )

    # Existing approach
    ax.text(
        0.245,
        0.79,
        "기존 방식",
        ha="center",
        va="center",
        fontsize=16,
        fontweight="bold",
        color="#8a2730",
    )
    add_box(
        ax,
        0.06,
        0.62,
        0.37,
        0.11,
        "모든 고객에게\n동일한 쿠폰/광고 발송",
        "#ffe6e8",
        "#d65b64",
    )
    add_box(
        ax,
        0.06,
        0.45,
        0.37,
        0.11,
        "구매 가능성이 낮은 고객,\n예상 구매금액이 작은 고객에게도 비용 투입",
        "#fff1d8",
        "#e2a642",
        fontsize=12,
    )
    add_box(
        ax,
        0.06,
        0.28,
        0.37,
        0.11,
        "마케팅 비용 낭비\n수익성 낮은 캠페인",
        "#f9d4d8",
        "#ca4c58",
    )
    add_arrow(ax, (0.245, 0.62), (0.245, 0.56), "#b05a62")
    add_arrow(ax, (0.245, 0.45), (0.245, 0.39), "#b05a62")

    # Proposed approach
    ax.text(
        0.755,
        0.79,
        "본 프로젝트 방식",
        ha="center",
        va="center",
        fontsize=16,
        fontweight="bold",
        color="#195a4a",
    )
    add_box(
        ax,
        0.57,
        0.62,
        0.37,
        0.11,
        "RandomForestClassifier\n고객별 구매확률 예측",
        "#e0f4ee",
        "#4aa282",
    )
    add_box(
        ax,
        0.57,
        0.45,
        0.37,
        0.11,
        "RandomForestRegressor\n고객별 예상 구매금액 예측",
        "#e5eefb",
        "#5b82c4",
    )
    add_box(
        ax,
        0.57,
        0.28,
        0.37,
        0.11,
        "Expected Profit 기준\n마케팅 우선 고객 선정",
        "#dcf3df",
        "#4c9f5a",
    )
    add_arrow(ax, (0.755, 0.62), (0.755, 0.56), "#516173")
    add_arrow(ax, (0.755, 0.45), (0.755, 0.39), "#516173")

    # Central formula
    add_box(
        ax,
        0.285,
        0.075,
        0.43,
        0.105,
        "Expected_Profit = Purchase_Probability x Predicted_Sales_Amount - 5,000",
        "#ffffff",
        "#2f3a4a",
        fontsize=12,
    )
    add_arrow(ax, (0.43, 0.335), (0.57, 0.335), "#2f3a4a")
    ax.text(
        0.5,
        0.365,
        "단순 발송에서\n수익성 기반 선별로 전환",
        ha="center",
        va="center",
        fontsize=12,
        fontweight="bold",
        color="#2f3a4a",
        linespacing=1.35,
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT_PATH, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
