import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from flask import Flask, Response, jsonify, render_template, request

app = Flask(__name__)

API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000").rstrip("/")


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "analytics"})


@app.get("/api/<path:api_path>")
def proxy_api(api_path):
    # Forward only GET requests to the configured internal API.
    target = f"{API_BASE_URL}/api/{api_path}"
    if request.query_string:
        target += "?" + request.query_string.decode("ascii", errors="ignore")

    upstream_request = Request(
        target,
        headers={"Accept": "application/json"},
        method="GET",
    )

    try:
        with urlopen(upstream_request, timeout=20) as upstream:
            payload = upstream.read()
            content_type = upstream.headers.get(
                "Content-Type", "application/json"
            )
            return Response(
                payload,
                status=upstream.status,
                content_type=content_type,
            )
    except HTTPError as exc:
        return Response(
            exc.read(),
            status=exc.code,
            content_type=exc.headers.get(
                "Content-Type", "application/json"
            ),
        )
    except (URLError, TimeoutError) as exc:
        app.logger.warning("API request failed: %s", exc)
        return jsonify({
            "detail": "Analytics API is unavailable. Check the API container."
        }), 502


if __name__ == "__main__":
    port = int(os.getenv("ANALYTICS_PORT", "8502"))
    app.run(host="0.0.0.0", port=port, debug=False)