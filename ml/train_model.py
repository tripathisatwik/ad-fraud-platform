import json
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import precision_recall_curve, average_precision_score

sys.path.append(str(Path(__file__).resolve().parent))
from features import FEATURE_COLUMNS, build_features

DATASET_PATH = Path("data/raw/train_sample.csv")
MODEL_DIR = Path("ml/models")
MODEL_PATH = MODEL_DIR / "ad_fraud_xgboost.json"
FEATURE_SCHEMA_PATH = MODEL_DIR / "feature_schema.json"

SCALE_POS_WEIGHT_CANDIDATES = [10, 50, 100, 200]

def calculate_metrics(y_true, predictions):
    y_true = y_true.reset_index(drop=True)
    predictions = pd.Series(predictions).reset_index(drop=True)
    
    tp = int(((y_true == 1) & (predictions == 1)).sum())
    fp = int(((y_true == 0) & (predictions == 1)).sum())
    fn = int(((y_true == 1) & (predictions == 0)).sum())
    tn = int(((y_true == 0) & (predictions == 0)).sum())
    
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    accuracy = (tp + tn) / len(y_true) if len(y_true) > 0 else 0.0
    
    return {"accuracy": accuracy, "precision": precision, "recall": recall, "f1": f1,
            "true_positive": tp, "false_positive": fp, "false_negative": fn, "true_negative": tn}

def pr_auc(y_true, probs):
    return float(average_precision_score(y_true, probs))

def find_best_threshold(y_true, probs):
    precision, recall, thresholds = precision_recall_curve(y_true, probs)
    f1_scores = 2 * precision * recall / (precision + recall + 1e-9)
    best_idx = f1_scores[:-1].argmax()
    return float(thresholds[best_idx]), float(f1_scores[best_idx])

def main():
    print("Loading dataset...")
    df = pd.read_csv(DATASET_PATH, parse_dates=["click_time", "attributed_time"])
    print(f"Rows loaded: {len(df):,}")
    
    print("\nBuilding features...")
    df = build_features(df)
    print("Feature engineering complete.")
    

    # Chronological split (80% Train, 20% Test)
    split_index = int(len(df) * 0.80)
    train_df = df.iloc[:split_index].copy()
    test_df = df.iloc[split_index:].copy()

    val_split_index = int(len(train_df) * 0.90)
    fit_df = train_df.iloc[:val_split_index]
    val_df = train_df.iloc[val_split_index:]
    
    X_fit, y_fit = fit_df[FEATURE_COLUMNS], fit_df["is_attributed"]
    X_val, y_val = val_df[FEATURE_COLUMNS], val_df["is_attributed"]
    X_train, y_train = train_df[FEATURE_COLUMNS], train_df["is_attributed"]
    X_test, y_test = test_df[FEATURE_COLUMNS], test_df["is_attributed"]
    

    # scale_pos_weight sweep
    print("\n=== SCALE_POS_WEIGHT SWEEP ===")
    best_spw = 200
    best_val_prauc = -1.0
    
    for spw in SCALE_POS_WEIGHT_CANDIDATES:
        candidate = xgb.XGBClassifier(
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
            scale_pos_weight=spw,
            early_stopping_rounds=50,
            random_state=42,
        )
        candidate.fit(X_fit, y_fit, eval_set=[(X_val, y_val)], verbose=False)
        val_probs = candidate.predict_proba(X_val)[:, 1]
        val_prauc = pr_auc(y_val, val_probs)
        
        print(f"scale_pos_weight={spw:>10} -> val PR-AUC: {val_prauc:.4f}")
        if val_prauc > best_val_prauc:
            best_val_prauc = val_prauc
            best_spw = spw
            
    print(f"\nBest scale_pos_weight: {best_spw} (val PR-AUC {best_val_prauc:.4f})")
    
    # Final XGBoost Model
    model = xgb.XGBClassifier(
        n_estimators=1000, max_depth=4, min_child_weight=5, gamma=0.1,
        learning_rate=0.05, subsample=0.8, colsample_bytree=0.8,
        objective="binary:logistic", eval_metric="aucpr", tree_method="hist",
        scale_pos_weight=best_spw, early_stopping_rounds=50, random_state=42,
    )
    
    print("\nTraining final XGBoost model...")
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    print("Training complete.")
    
    # Threshold tuning
    val_probabilities = model.predict_proba(X_val)[:, 1]
    best_threshold, best_val_f1 = find_best_threshold(y_val, val_probabilities)
    print(f"\nBest threshold by F1 (on Val set): {best_threshold:.4f} (Val F1 {best_val_f1:.4f})")
    
    # Evaluate on Test set & Save
    test_probabilities = model.predict_proba(X_test)[:, 1]
    predictions = (test_probabilities >= best_threshold).astype(int)
    metrics = calculate_metrics(y_test, predictions)
    test_prauc_manual = pr_auc(y_test, test_probabilities)
    
    print("\n=== MODEL RESULTS (tuned threshold) ===")
    print(f"Accuracy:  {metrics['accuracy']:.4f}")
    print(f"Precision: {metrics['precision']:.4f}")
    print(f"Recall:    {metrics['recall']:.4f}")
    print(f"F1 Score:  {metrics['f1']:.4f}")
    print(f"Test PR-AUC (manual): {test_prauc_manual:.4f}")
    
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
        "train_split": 0.80,
        "split_method": "chronological",
        "imbalance_handling": "scale_pos_weight",
        "scale_pos_weight": best_spw,
        "prediction_threshold": best_threshold,
    }
    with open(FEATURE_SCHEMA_PATH, "w", encoding="utf-8") as file:
        json.dump(schema, file, indent=4)
    print(f"Feature schema saved: {FEATURE_SCHEMA_PATH}")
    
    importance = (
        pd.Series(model.feature_importances_, index=FEATURE_COLUMNS)
        .sort_values(ascending=False)
    )
    print("\n=== FEATURE IMPORTANCE ===")
    print(importance)

if __name__ == "__main__":
    main()