import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import average_precision_score, precision_recall_curve

sys.path.append(str(Path(__file__).resolve().parent))
from features import FEATURE_COLUMNS, build_features

DATASET_PATH = Path("data/raw/train_sample.csv")
MODEL_DIR = Path("ml/models")
MODEL_PATH = MODEL_DIR / "ad_fraud_xgboost.json"
FEATURE_SCHEMA_PATH = MODEL_DIR / "feature_schema.json"

SCALE_POS_WEIGHT_CANDIDATES = [10, 50, 100, 200]
RANDOM_STATE = 42


def calculate_metrics(y_true, predictions):
    y_true = pd.Series(y_true).reset_index(drop=True).astype(int)
    predictions = pd.Series(predictions).reset_index(drop=True).astype(int)

    tp = int(((y_true == 1) & (predictions == 1)).sum())
    fp = int(((y_true == 0) & (predictions == 1)).sum())
    fn = int(((y_true == 1) & (predictions == 0)).sum())
    tn = int(((y_true == 0) & (predictions == 0)).sum())

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    accuracy = (tp + tn) / len(y_true) if len(y_true) else 0.0

    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "true_negative": tn,
    }


def pr_auc(y_true, probabilities):
    return float(average_precision_score(y_true, probabilities))


def find_best_threshold(y_true, probabilities):
    precision, recall, thresholds = precision_recall_curve(y_true, probabilities)
    if len(thresholds) == 0:
        raise ValueError("Cannot select a threshold: validation set has no usable thresholds.")

    f1_scores = 2 * precision * recall / (precision + recall + 1e-9)
    best_idx = int(np.nanargmax(f1_scores[:-1]))
    return float(thresholds[best_idx]), float(f1_scores[best_idx])

def make_model(scale_pos_weight):
    return xgb.XGBClassifier(
        n_estimators=1000,
        max_depth=4,
        min_child_weight=5,
        gamma=0.1,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="binary:logistic",
        eval_metric="aucpr",
        tree_method="hist",
        scale_pos_weight=scale_pos_weight,
        early_stopping_rounds=50,
        random_state=RANDOM_STATE,
    )


def report_split(name, frame):
    positives = int(frame["is_attributed"].sum())
    negatives = int(len(frame) - positives)
    print(
        f"{name:>12}: rows={len(frame):,}, positives={positives:,}, "
        f"negatives={negatives:,}, prevalence={positives / len(frame):.4%}"
    )


def main():
    print("Loading dataset...")
    df = pd.read_csv(DATASET_PATH, parse_dates=["click_time", "attributed_time"])
    print(f"Rows loaded: {len(df):,}")

    required_columns = {"click_time", "is_attributed", *FEATURE_COLUMNS}
    missing_raw = {"click_time", "is_attributed"} - set(df.columns)
    if missing_raw:
        raise ValueError(f"Dataset is missing required columns: {sorted(missing_raw)}")

    print("\nBuilding features...")
    df = build_features(df)
    print("Feature engineering complete.")

    n_rows = len(df)
    fit_end = int(n_rows * 0.72)
    val_end = int(n_rows * 0.80)

    fit_df = df.iloc[:fit_end].copy()
    val_df = df.iloc[fit_end:val_end].copy()
    test_df = df.iloc[val_end:].copy()

    if min(len(fit_df), len(val_df), len(test_df)) == 0:
        raise ValueError("Dataset is too small for the requested chronological split.")

    print("\n=== CHRONOLOGICAL SPLITS ===")
    report_split("Fit", fit_df)
    report_split("Validation", val_df)
    report_split("Test", test_df)

    for name, frame in (("fit", fit_df), ("validation", val_df), ("test", test_df)):
        if frame["is_attributed"].nunique() < 2:
            raise ValueError(
                f"The {name} split contains only one target class. "
                "Adjust split boundaries or use a larger/more representative dataset."
            )

    if "--check-only" in sys.argv:
        print("\\nSplit sanity check passed. No model was trained or overwritten.")
        return

    X_fit, y_fit = fit_df[FEATURE_COLUMNS], fit_df["is_attributed"].astype(int)
    X_val, y_val = val_df[FEATURE_COLUMNS], val_df["is_attributed"].astype(int)
    X_test, y_test = test_df[FEATURE_COLUMNS], test_df["is_attributed"].astype(int)

    if list(X_fit.columns) != FEATURE_COLUMNS:
        raise ValueError("Feature columns do not match FEATURE_COLUMNS order.")
    if not (list(X_val.columns) == list(X_test.columns) == FEATURE_COLUMNS):
        raise ValueError("Fit, validation, and test feature columns/order do not match.")

    print("\n=== SCALE_POS_WEIGHT SWEEP ===")
    best_spw = None
    best_val_prauc = -1.0

    for spw in SCALE_POS_WEIGHT_CANDIDATES:
        candidate = make_model(spw)
        candidate.fit(X_fit, y_fit, eval_set=[(X_val, y_val)], verbose=False)
        val_probabilities = candidate.predict_proba(X_val)[:, 1]
        val_prauc = pr_auc(y_val, val_probabilities)

        print(f"scale_pos_weight={spw:>10} -> validation PR-AUC: {val_prauc:.4f}")
        if val_prauc > best_val_prauc:
            best_val_prauc = val_prauc
            best_spw = spw

    print(f"\nBest scale_pos_weight: {best_spw} (validation PR-AUC {best_val_prauc:.4f})")

    model = make_model(best_spw)
    print("\nTraining final model on fit split only...")
    model.fit(X_fit, y_fit, eval_set=[(X_val, y_val)], verbose=False)
    print("Training complete.")

    val_probabilities = model.predict_proba(X_val)[:, 1]
    best_threshold, best_val_f1 = find_best_threshold(y_val, val_probabilities)
    print(
        f"\nBest threshold by F1 (validation only): {best_threshold:.6f} "
        f"(validation F1 {best_val_f1:.4f})"
    )

    # Final evaluation on the untouched chronological test period.
    test_probabilities = model.predict_proba(X_test)[:, 1]
    test_predictions = (test_probabilities >= best_threshold).astype(int)
    metrics = calculate_metrics(y_test, test_predictions)
    test_prauc = pr_auc(y_test, test_probabilities)
    prevalence = float(y_test.mean())
    all_negative_accuracy = float((y_test == 0).mean())

    print("\n=== TEST RESULTS (threshold chosen on validation) ===")
    print(f"Test positive prevalence: {prevalence:.4%}")
    print(f"All-negative baseline accuracy: {all_negative_accuracy:.4f}")
    print(f"Accuracy:  {metrics['accuracy']:.4f}")
    print(f"Precision: {metrics['precision']:.4f}")
    print(f"Recall:    {metrics['recall']:.4f}")
    print(f"F1 Score:  {metrics['f1']:.4f}")
    print(f"Test PR-AUC: {test_prauc:.4f}")

    print("\n=== CONFUSION MATRIX ===")
    print(f"True Positives:  {metrics['true_positive']:,}")
    print(f"False Positives: {metrics['false_positive']:,}")
    print(f"False Negatives: {metrics['false_negative']:,}")
    print(f"True Negatives:  {metrics['true_negative']:,}")

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model.save_model(MODEL_PATH)
    print(f"\nModel saved: {MODEL_PATH}")

    schema = {
        "features": FEATURE_COLUMNS,
        "target": "is_attributed",
        "model_type": "XGBClassifier",
        "training_dataset": str(DATASET_PATH),
        "split_method": "chronological",
        "fit_split": 0.72,
        "validation_split": 0.08,
        "test_split": 0.20,
        "imbalance_handling": "scale_pos_weight",
        "scale_pos_weight": best_spw,
        "prediction_threshold": best_threshold,
        "threshold_selection_metric": "F1 on validation split",
        "validation_pr_auc": best_val_prauc,
        "validation_f1_at_selected_threshold": best_val_f1,
        "test_metrics": metrics,
        "test_pr_auc": test_prauc,
        "test_positive_prevalence": prevalence,
        "all_negative_baseline_accuracy": all_negative_accuracy,
        "random_state": RANDOM_STATE,
        "model_fit_rows": len(fit_df),
        "validation_rows": len(val_df),
        "test_rows": len(test_df),
    }

    # Write schema to a temporary file first, then replace, avoiding a partial JSON file.
    temp_schema_path = FEATURE_SCHEMA_PATH.with_suffix(".json.tmp")
    with open(temp_schema_path, "w", encoding="utf-8") as file:
        json.dump(schema, file, indent=4)
    temp_schema_path.replace(FEATURE_SCHEMA_PATH)
    print(f"Feature schema saved: {FEATURE_SCHEMA_PATH}")

    importance = pd.Series(model.feature_importances_, index=FEATURE_COLUMNS).sort_values(
        ascending=False
    )
    print("\n=== FEATURE IMPORTANCE ===")
    print(importance)


if __name__ == "__main__":
    main()