"""Anonymous telemetry for tracking global cleanup impact."""

import os
import json
import uuid
import platform
import logging
import threading
from typing import Dict, Any, Optional
from datetime import datetime
import urllib.request
import urllib.error

# Placeholder URL - User needs to deploy the server function first
DEFAULT_TELEMETRY_URL = "https://your-cloud-function-url.cloudfunctions.net/jhadoo-telemetry"

logger = logging.getLogger(__name__)


class TelemetryClient:
    """Handles anonymous telemetry reporting."""

    def __init__(self, config: Any):
        self.config = config
        self.enabled = config.get("telemetry", {}).get("enabled", True)
        self.url = config.get("telemetry", {}).get("url", DEFAULT_TELEMETRY_URL)
        self.user_id = self._get_or_create_user_id()

    def _get_or_create_user_id(self) -> str:
        """Get existing user ID or generate a new anonymous UUID."""
        config_dir = os.path.dirname(self.config.get("logging", {}).get("log_file", ""))
        # Fallback to home if config_dir is empty/invalid
        if not config_dir:
            config_dir = os.path.expanduser("~/.jhadoo")
            
        id_file = os.path.join(config_dir, "telemetry_id.json")
        
        try:
            if os.path.exists(id_file):
                with open(id_file, 'r') as f:
                    data = json.load(f)
                    return data.get("user_id", str(uuid.uuid4()))
            
            # Create new ID
            new_id = str(uuid.uuid4())
            os.makedirs(os.path.dirname(id_file), exist_ok=True)
            with open(id_file, 'w') as f:
                json.dump({"user_id": new_id, "created": datetime.now().isoformat()}, f)
            return new_id
            
        except Exception:
            return "unknown-user"

    def send_stats(self, bytes_saved: int, duration_seconds: float):
        """Send cleanup statistics asynchronously."""
        if not self.enabled:
            return

        payload = {
            "user_id": self.user_id,
            "bytes_saved": bytes_saved,
            "duration_seconds": duration_seconds,
            "timestamp": datetime.now().isoformat(),
            "os": platform.system(),
            "version": "1.1.0", # TODO: Get dynamically
            "python_version": platform.python_version()
        }

        # Run in a separate thread to not block CLI
        thread = threading.Thread(target=self._send_request, args=(payload,))
        thread.daemon = True
        thread.start()

    def _send_request(self, payload: Dict[str, Any]):
        """Internal method to send HTTP request."""
        try:
            # Check if URL is configured
            if "your-cloud-function-url" in self.url:
                return # Silently fail if not configured

            data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                self.url,
                data=data,
                headers={'Content-Type': 'application/json'}
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status != 200:
                    logger.debug(f"Telemetry failed with status {response.status}")
        except Exception as e:
            # Fail silently, never annoy user with network errors for telemetry
            logger.debug(f"Telemetry failed: {e}")
