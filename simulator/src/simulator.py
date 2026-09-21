from datetime import datetime, timezone
import json
import uuid


event = {
    "event_id": str(uuid.uuid4()),
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "user_id": "user_001",
    "ip_address": "192.168.1.42",
    "ad_id": "ad_001",
    "event_type": "click",
}

print(json.dumps(event))