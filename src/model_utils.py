"""모델 저장, 평가 시각화, Feature Importance 저장에 사용하는 공통 함수입니다."""

from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.metrics import ConfusionMatrixDisplay


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODELS_DIR = PROJECT_ROOT / "models"
FIGURES_DIR = PROJECT_ROOT / "outputs" / "figures"


def save_joblib(obj, filename: str) -> Path:
    """학습된 모델이나 필요한 객체를 models 폴더에 저장합니다."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    path = MODELS_DIR / filename
    joblib.dump(obj, path)
    return path


def save_confusion_matrix(y_true, y_pred, filename: str = "confusion_matrix.png") -> Path:
    """분류 모델의 Confusion Matrix 이미지를 저장합니다."""
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    path = FIGURES_DIR / filename
    ConfusionMatrixDisplay.from_predictions(y_true, y_pred, cmap="Blues")
    plt.title("Confusion Matrix")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()
    return path


def save_feature_importance(pipeline, filename: str) -> Path:
    """Pipeline 안의 RandomForest 모델에서 Feature Importance를 뽑아 저장합니다."""
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    path = FIGURES_DIR / filename

    feature_names = pipeline.named_steps["preprocessor"].get_feature_names_out()
    importances = pipeline.named_steps["model"].feature_importances_
    table = (
        pd.DataFrame({"feature": feature_names, "importance": importances})
        .sort_values("importance", ascending=False)
        .head(15)
    )

    plt.figure(figsize=(9, 6))
    sns.barplot(data=table, x="importance", y="feature")
    plt.title("Top 15 Feature Importance")
    plt.tight_layout()
    plt.savefig(path, dpi=150)
    plt.close()
    return path
