import html
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fraud-dashboard")

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", "http://floci:4566")
S3_BUCKET = os.getenv("S3_BUCKET", "ad-fraud-data")
S3_PREFIX = os.getenv("S3_PREFIX", "predictions/")
PORT = int(os.getenv("DASHBOARD_PORT", "8501"))
MAX_OBJECTS = int(os.getenv("DASHBOARD_MAX_OBJECTS", "100"))
MAX_RECORDS = int(os.getenv("DASHBOARD_MAX_RECORDS", "10000"))
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.7419291138648987"))

s3 = boto3.client("s3", endpoint_url=AWS_ENDPOINT_URL, region_name=AWS_REGION,
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"))

def load_predictions():
    # Retrieve every S3 object under the prediction prefix.
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX,
    ):
        objects.extend(page.get("Contents", []))

    # Process newest objects first.
    objects.sort(
        key=lambda item: item["LastModified"],
        reverse=True,
    )

    records = []
    objects_read = 0

    for item in objects:
        if len(records) >= MAX_RECORDS:
            break

        try:
            obj = s3.get_object(
                Bucket=S3_BUCKET,
                Key=item["Key"],
            )
            payload = obj["Body"].read().decode(
                "utf-8",
                errors="replace",
            )
            objects_read += 1

            for line in payload.splitlines():
                if not line.strip():
                    continue

                try:
                    record = json.loads(line)

                    if (
                        "fraud_probability" in record
                        and "prediction" in record
                    ):
                        records.append(record)

                        if len(records) >= MAX_RECORDS:
                            break

                except json.JSONDecodeError:
                    log.warning(
                        "Skipping malformed JSON line in %s",
                        item["Key"],
                    )

        except (BotoCoreError, ClientError) as exc:
            log.warning(
                "Could not read S3 object %s: %s",
                item["Key"],
                exc,
            )

    records.sort(
        key=lambda r: str(
            r.get("timestamp", r.get("event_time", ""))
        ),
        reverse=True,
    )

    return records, objects_read

def esc(value):
    return html.escape(str(value if value is not None else ""))

def dashboard_html():
    try:
        records, objects_read = load_predictions()
        total = len(records)
        flagged = sum(1 for r in records if int(r.get("prediction", 0)) == 1)
        fraud_rate = (100.0 * flagged / total) if total else 0.0
        avg_probability = sum(float(r.get("fraud_probability", 0)) for r in records) / total if total else 0.0
        max_probability = max((float(r.get("fraud_probability", 0)) for r in records), default=0.0)
        latest_time = str(records[0].get("timestamp", records[0].get("event_time", "—"))) if records else "—"
        hourly = {}
        for r in records:
            stamp = str(r.get("timestamp", r.get("event_time", "")))
            hour = stamp[:13].replace("T", " ") or "Unknown"
            row = hourly.setdefault(hour, {"total": 0, "flagged": 0, "probability_sum": 0.0})
            row["total"] += 1
            row["flagged"] += int(r.get("prediction", 0))
            row["probability_sum"] += float(r.get("fraud_probability", 0))
        hourly_rows = sorted(hourly.items())[-12:]
        max_hour_count = max((v["total"] for _, v in hourly_rows), default=1) or 1
        hourly_html = "".join(
            f'<tr><td>{esc(hour)}</td><td>{v["total"]:,}</td><td>{v["flagged"]:,}</td>'
            f'<td>{v["probability_sum"] / v["total"]:.4f}</td>'
            f'<td><div class="bar"><span style="width:{100*v["total"]/max_hour_count:.1f}%"></span></div></td></tr>'
            for hour, v in hourly_rows) or '<tr><td colspan="5">No prediction records found.</td></tr>'
        recent_html = "".join(
            '<tr>'
            f'<td>{esc(r.get("timestamp", r.get("event_time", "—")))}</td>'
            f'<td>{esc(r.get("event_id", "—"))}</td><td>{esc(r.get("ip", "—"))}</td>'
            f'<td>{float(r.get("fraud_probability", 0)):.4f}</td>'
            f'<td><span class="pill {"fraud" if int(r.get("prediction", 0)) == 1 else "legit"}">'
            f'{"FRAUD" if int(r.get("prediction", 0)) == 1 else "NOT FRAUD"}</span></td></tr>'
            for r in records[:25]) or '<tr><td colspan="5">No prediction records found.</td></tr>'
        status = f"Loaded {total:,} prediction records from {objects_read} recent S3 objects."
        error_banner = ""
    except Exception as exc:
        log.exception("Dashboard refresh failed")
        total = flagged = 0
        fraud_rate = avg_probability = max_probability = 0.0
        latest_time = "Unavailable"
        hourly_html = '<tr><td colspan="5">Analytics are temporarily unavailable.</td></tr>'
        recent_html = '<tr><td colspan="5">Could not load predictions from S3.</td></tr>'
        status = "S3 read failed. Check dashboard logs and Floci connectivity."
        error_banner = f'<div class="error">{esc(type(exc).__name__)}: {esc(exc)}</div>'
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><meta http-equiv="refresh" content="30">
<title>Ad Fraud Monitor</title><style>
:root{{--bg:#0b1020;--panel:#131b2e;--line:#283550;--muted:#9aa8c2;--text:#edf2ff;--blue:#6ba7ff;--red:#ff6b78;--green:#54d6a0}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:15px/1.5 system-ui,sans-serif}}
main{{max-width:1400px;margin:auto;padding:28px}}header{{display:flex;justify-content:space-between;gap:16px;align-items:center;flex-wrap:wrap;margin-bottom:24px}}
h1{{font-size:clamp(25px,3vw,36px);margin:0;letter-spacing:-.04em}}.subtitle,.muted{{color:var(--muted)}}
.refresh{{border:1px solid var(--line);padding:8px 12px;border-radius:9px;color:var(--muted)}}
.cards{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:18px 0 22px}}
.card,.panel{{background:var(--panel);border:1px solid var(--line);border-radius:15px;padding:20px;min-width:0}}
.card-label{{color:var(--muted);font-size:13px}}.metric{{font-size:clamp(25px,3vw,34px);font-weight:750;letter-spacing:-.04em;margin:8px 0 2px;overflow-wrap:anywhere}}
.red{{color:var(--red)}}.blue{{color:var(--blue)}}.green{{color:var(--green)}}.grid{{display:grid;grid-template-columns:1fr;gap:18px}}
h2{{font-size:18px;margin:0 0 14px}}.table-wrap{{overflow-x:auto}}table{{width:100%;border-collapse:collapse;white-space:nowrap}}
th,td{{text-align:left;padding:11px 12px;border-bottom:1px solid var(--line)}}th{{color:var(--muted);font-size:12px;font-weight:650;text-transform:uppercase;letter-spacing:.06em}}
tr:last-child td{{border-bottom:0}}.pill{{display:inline-block;border-radius:999px;padding:3px 9px;font-size:11px;font-weight:750;letter-spacing:.04em}}
.fraud{{background:#48232e;color:#ff9ba4}}.legit{{background:#173b35;color:#7ce2bc}}
.bar{{height:7px;min-width:90px;background:#222e47;border-radius:10px;overflow:hidden}}.bar span{{display:block;height:100%;background:var(--blue);border-radius:10px}}
.foot{{color:var(--muted);font-size:12px;margin-top:18px}}.error{{padding:12px;border:1px solid #8c3845;border-radius:10px;margin:12px 0;color:#ffabb2;overflow-wrap:anywhere}}
@media(max-width:850px){{.cards{{grid-template-columns:repeat(2,minmax(0,1fr))}}main{{padding:16px}}}}
@media(max-width:460px){{.cards{{gap:8px}}.card{{padding:13px}}}}
</style></head><body><main>
<header><div><h1>Ad Fraud Monitor</h1><div class="subtitle">Streaming model predictions · S3-backed view</div></div><div class="refresh">Auto-refreshes every 30 seconds</div></header>
{error_banner}<section class="cards">
<div class="card"><div class="card-label">Predictions loaded</div><div class="metric">{total:,}</div><div class="muted">Recent S3 objects</div></div>
<div class="card"><div class="card-label">Flagged fraud</div><div class="metric red">{flagged:,}</div><div class="muted">{fraud_rate:.2f}% of loaded records</div></div>
<div class="card"><div class="card-label">Average probability</div><div class="metric blue">{avg_probability:.4f}</div><div class="muted">Across loaded records</div></div>
<div class="card"><div class="card-label">Maximum probability</div><div class="metric {"red" if max_probability >= FRAUD_THRESHOLD else "green"}">{max_probability:.4f}</div><div class="muted">Decision threshold: {FRAUD_THRESHOLD:.4f}</div></div>
</section><section class="grid">
<div class="panel"><h2>Hourly activity</h2><div class="table-wrap"><table><thead><tr><th>Event hour</th><th>Events</th><th>Flagged</th><th>Avg probability</th><th>Volume</th></tr></thead><tbody>{hourly_html}</tbody></table></div></div>
<div class="panel"><h2>Recent predictions</h2><div class="table-wrap"><table><thead><tr><th>Timestamp</th><th>Event ID</th><th>IP</th><th>Probability</th><th>Prediction</th></tr></thead><tbody>{recent_html}</tbody></table></div></section>
<div class="foot">{esc(status)} Last event timestamp shown: {esc(latest_time)}. Dashboard uses up to {MAX_OBJECTS} newest S3 objects and {MAX_RECORDS:,} records per refresh. This is a monitoring view, not a model-quality evaluation.</div>
</main></body></html>"""

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            body, content_type, status = b'{"status":"ok"}', "application/json", 200
        elif path in ("/", "/index.html"):
            body, content_type, status = dashboard_html().encode("utf-8"), "text/html; charset=utf-8", 200
        else:
            body, content_type, status = b'{"error":"not found"}', "application/json", 404
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, fmt, *args):
        log.info("%s - %s", self.address_string(), fmt % args)

if __name__ == "__main__":
    log.info("Starting dashboard on port %s; S3 endpoint %s; bucket %s", PORT, AWS_ENDPOINT_URL, S3_BUCKET)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()