from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "processed" / "customer_features.csv"
MODELS_DIR = PROJECT_ROOT / "models"
REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
FIGURES_DIR = PROJECT_ROOT / "outputs" / "figures"

CLASSIFIER_PATH = MODELS_DIR / "rf_classifier.pkl"
CLASSIFIER_FEATURES_PATH = MODELS_DIR / "classifier_feature_columns.pkl"
REGRESSOR_PATH = MODELS_DIR / "rf_regressor.pkl"
REGRESSOR_FEATURES_PATH = MODELS_DIR / "regressor_feature_columns.pkl"

FULL_SCORED_PATH = REPORTS_DIR / "all_customers_expected_profit.csv"
SUMMARY_PATH = REPORTS_DIR / "probability_vs_expected_profit_summary.csv"
SCATTER_PATH = FIGURES_DIR / "purchase_probability_vs_expected_profit.png"
BAR_PATH = FIGURES_DIR / "probability_vs_expected_profit_strategy_comparison.png"

MARKETING_COST = 5000
TOP_RATIO = 0.2


def find_customer_id_col(df: pd.DataFrame) -> str | None:
    for candidate in ["CustomerKey", "Customer ID", "CustomerID", "Customer"]:
        if candidate in df.columns:
            return candidate
    return None


def score_customers() -> pd.DataFrame:
    df = pd.read_csv(DATA_PATH)
    classifier = joblib.load(CLASSIFIER_PATH)
    classifier_features = joblib.load(CLASSIFIER_FEATURES_PATH)
    regressor = joblib.load(REGRESSOR_PATH)
    regressor_features = joblib.load(REGRESSOR_FEATURES_PATH)

    scored = df.copy()
    if "Buy_Label" not in scored.columns and "Buy" in scored.columns:
        scored = scored.rename(columns={"Buy": "Buy_Label"})

    scored["Purchase_Probability"] = classifier.predict_proba(
        scored[classifier_features]
    )[:, 1]
    scored["Predicted_Sales_Amount"] = regressor.predict(scored[regressor_features])
    scored["Predicted_Sales_Amount"] = scored["Predicted_Sales_Amount"].clip(lower=0)
    scored["Marketing_Cost"] = MARKETING_COST
    scored["Expected_Profit"] = (
        scored["Purchase_Probability"] * scored["Predicted_Sales_Amount"]
        - scored["Marketing_Cost"]
    )
    return scored


def build_comparison(scored: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    target_count = max(1, int(len(scored) * TOP_RATIO))

    probability_top = scored.sort_values(
        ["Purchase_Probability", "Predicted_Sales_Amount"],
        ascending=[False, False],
    ).head(target_count)
    profit_top = scored.sort_values("Expected_Profit", ascending=False).head(target_count)

    customer_id_col = find_customer_id_col(scored)
    probability_ids = set(probability_top[customer_id_col]) if customer_id_col else set()
    profit_ids = set(profit_top[customer_id_col]) if customer_id_col else set()
    overlap_count = len(probability_ids & profit_ids) if customer_id_col else 0

    summary = pd.DataFrame(
        [
            {
                "strategy": "Purchase Probability Top 20%",
                "target_count": len(probability_top),
                "avg_purchase_probability": probability_top["Purchase_Probability"].mean(),
                "avg_predicted_sales_amount": probability_top[
                    "Predicted_Sales_Amount"
                ].mean(),
                "avg_expected_profit": probability_top["Expected_Profit"].mean(),
                "total_expected_profit": probability_top["Expected_Profit"].sum(),
                "positive_expected_profit_count": (
                    probability_top["Expected_Profit"] > 0
                ).sum(),
                "min_expected_profit": probability_top["Expected_Profit"].min(),
                "max_expected_profit": probability_top["Expected_Profit"].max(),
                "overlap_with_other_strategy": overlap_count,
            },
            {
                "strategy": "Expected Profit Top 20%",
                "target_count": len(profit_top),
                "avg_purchase_probability": profit_top["Purchase_Probability"].mean(),
                "avg_predicted_sales_amount": profit_top[
                    "Predicted_Sales_Amount"
                ].mean(),
                "avg_expected_profit": profit_top["Expected_Profit"].mean(),
                "total_expected_profit": profit_top["Expected_Profit"].sum(),
                "positive_expected_profit_count": (
                    profit_top["Expected_Profit"] > 0
                ).sum(),
                "min_expected_profit": profit_top["Expected_Profit"].min(),
                "max_expected_profit": profit_top["Expected_Profit"].max(),
                "overlap_with_other_strategy": overlap_count,
            },
        ]
    )

    scored["Selected_By"] = "Not selected"
    if customer_id_col:
        scored.loc[scored[customer_id_col].isin(probability_ids), "Selected_By"] = (
            "Probability Top 20%"
        )
        scored.loc[scored[customer_id_col].isin(profit_ids), "Selected_By"] = (
            "Expected Profit Top 20%"
        )
        scored.loc[
            scored[customer_id_col].isin(probability_ids & profit_ids),
            "Selected_By",
        ] = "Both"

    return scored, summary


def save_scatter(scored: pd.DataFrame) -> None:
    plot_data = scored.copy()
    plot_data["Selected_For_Plot"] = plot_data["Selected_By"].where(
        plot_data["Selected_By"] != "Not selected", "Other customers"
    )

    palette = {
        "Other customers": "#c8cdd6",
        "Probability Top 20%": "#e28b43",
        "Expected Profit Top 20%": "#3e8f68",
        "Both": "#4f6fc7",
    }

    plt.figure(figsize=(12, 7))
    sns.scatterplot(
        data=plot_data,
        x="Purchase_Probability",
        y="Expected_Profit",
        hue="Selected_For_Plot",
        palette=palette,
        alpha=0.72,
        s=28,
        edgecolor=None,
    )
    plt.axhline(0, color="#333333", linestyle="--", linewidth=1.2)
    plt.title("Purchase Probability vs Expected Profit")
    plt.xlabel("Purchase Probability")
    plt.ylabel("Expected Profit")
    plt.legend(title="Selection Strategy", loc="best")
    plt.tight_layout()
    plt.savefig(SCATTER_PATH, dpi=150)
    plt.close()


def save_bar(summary: pd.DataFrame) -> None:
    plot_data = summary.melt(
        id_vars="strategy",
        value_vars=[
            "avg_expected_profit",
            "total_expected_profit",
            "positive_expected_profit_count",
        ],
        var_name="metric",
        value_name="value",
    )

    metric_labels = {
        "avg_expected_profit": "Average Expected Profit",
        "total_expected_profit": "Total Expected Profit",
        "positive_expected_profit_count": "Positive Profit Customers",
    }
    plot_data["metric"] = plot_data["metric"].map(metric_labels)

    g = sns.catplot(
        data=plot_data,
        x="strategy",
        y="value",
        col="metric",
        kind="bar",
        sharey=False,
        height=5,
        aspect=0.9,
        palette=["#e28b43", "#3e8f68"],
    )
    g.set_axis_labels("", "")
    g.set_titles("{col_name}")
    for ax in g.axes.flat:
        ax.tick_params(axis="x", rotation=18)
        for container in ax.containers:
            ax.bar_label(container, fmt="%.0f", padding=3, fontsize=9)
    g.fig.suptitle("Probability Top 20% vs Expected Profit Top 20%", y=1.05)
    g.fig.tight_layout()
    g.savefig(BAR_PATH, dpi=150)
    plt.close(g.fig)


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    scored = score_customers()
    scored, summary = build_comparison(scored)

    output_columns = [
        column
        for column in [
            find_customer_id_col(scored),
            "Buy_Label",
            "Purchase_Probability",
            "Predicted_Sales_Amount",
            "Marketing_Cost",
            "Expected_Profit",
            "Selected_By",
        ]
        if column is not None and column in scored.columns
    ]
    scored[output_columns].sort_values("Expected_Profit", ascending=False).to_csv(
        FULL_SCORED_PATH,
        index=False,
        encoding="utf-8-sig",
    )
    summary.to_csv(SUMMARY_PATH, index=False, encoding="utf-8-sig")
    save_scatter(scored)
    save_bar(summary)

    print(summary.to_string(index=False))
    print(f"Saved: {FULL_SCORED_PATH}")
    print(f"Saved: {SUMMARY_PATH}")
    print(f"Saved: {SCATTER_PATH}")
    print(f"Saved: {BAR_PATH}")


if __name__ == "__main__":
    main()
