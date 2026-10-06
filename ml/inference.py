import json
from pathlib import Path

import pandas as pd
import xgboost as xgb


PROJECT_ROOT = Path(__file__).resolve().parent.parent

MODEL_PATH = PROJECT_ROOT / "ml" / "models" / "ad_fraud_xgboost.json"
SCHEMA_PATH = PROJECT_ROOT / "ml" / "models" / "feature_schema.json"


class AdFraudInference:
    def __init__(self):
        print("Loading XGBoost model...")
        
        self.model = xgb.XGBClassifier()
        self.model.load_model(MODEL_PATH)

        print(f"Model loaded: {MODEL_PATH}")

        with open(SCHEMA_PATH, "r", encoding="utf-8") as file:
            schema = json.load(file)

        self.feature_columns = schema["features"]
        self.threshold = float(schema["prediction_threshold"])

        if len(self.feature_columns) != 23:
            raise ValueError(
                f"Expected 23 features, found {len(self.feature_columns)}"
            )

        print(f"Feature schema loaded: {len(self.feature_columns)} features")
        print(f"Prediction threshold: {self.threshold:.6f}")

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [
            feature
            for feature in self.feature_columns
            if feature not in df.columns
        ]

        if missing:
            raise ValueError(
                f"Missing model features: {missing}"
            )

        result = df.copy()

        X = result[self.feature_columns]

        probabilities = self.model.predict_proba(X)[:, 1]

        result["fraud_probability"] = probabilities
        result["prediction"] = (
            probabilities >= self.threshold
        ).astype(int)

        return result


_inference = None


def get_inference():
    global _inference

    if _inference is None:
        _inference = AdFraudInference()

    return _inference