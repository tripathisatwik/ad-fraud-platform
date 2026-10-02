#Quick verification that feature lists match between ml and spark
import re
import sys

with open("ml/features.py") as f:
    ml_text = f.read()
with open("spark/src/kafka_stream.py") as f:
    spark_text = f.read()

def extract_features(text):
    match = re.search(r"FEATURE_COLUMNS\s*=\s*\[(.*?)\]", text, re.DOTALL)
    return re.findall(r'"([^"]+)"', match.group(1))

ml_features = extract_features(ml_text)
spark_features = extract_features(spark_text)

print(f"ml/features.py:        {len(ml_features)} features")
print(f"spark/kafka_stream.py: {len(spark_features)} features")

if ml_features == spark_features:
    print("\nMATCH: Feature lists are identical (order + names)")
    for i, f in enumerate(ml_features, 1):
        print(f"  {i:2d}. {f}")
else:
    print("\nMISMATCH!")
    ml_set = set(ml_features)
    spark_set = set(spark_features)
    if ml_set - spark_set:
        print(f"  Missing from spark: {ml_set - spark_set}")
    if spark_set - ml_set:
        print(f"  Extra in spark:     {spark_set - ml_set}")
    sys.exit(1)