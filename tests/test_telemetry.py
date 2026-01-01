"""Tests for Telemetry module."""

import unittest
import json
import os
from unittest.mock import patch, MagicMock
from jhadoo.telemetry import TelemetryClient


class TestTelemetryClient(unittest.TestCase):
    
    def setUp(self):
        self.config = {
            "telemetry": {
                "enabled": True,
                "url": "http://mock-url"
            },
            "logging": {}
        }
        self.client = TelemetryClient(self.config)

    def test_user_id_generation(self):
        """Test that user ID is generated and persisted."""
        self.assertIsNotNone(self.client.user_id)
        self.assertNotEqual(self.client.user_id, "unknown-user")
        
        # Verify persistence
        config_dir = os.path.expanduser("~/.jhadoo")
        id_file = os.path.join(config_dir, "telemetry_id.json")
        self.assertTrue(os.path.exists(id_file))
        
        with open(id_file, 'r') as f:
            data = json.load(f)
            self.assertEqual(data['user_id'], self.client.user_id)

    @patch('urllib.request.urlopen')
    def test_send_stats(self, mock_urlopen):
        """Test sending stats."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response
        
        # We need to test the internal _send_request since send_stats is threaded
        payload = {
            "user_id": "test-id",
            "bytes_saved": 1000,
            "duration": 1.5,
            "timestamp": "2023-01-01",
            "os": "TestOS"
        }
        
        self.client._send_request(payload)
        
        # Verify call
        self.assertTrue(mock_urlopen.called)
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.full_url, "http://mock-url")
        
        sent_data = json.loads(req.data.decode('utf-8'))
        self.assertEqual(sent_data['bytes_saved'], 1000)

    def test_disabled_telemetry(self):
        """Test that disabled telemetry sends nothing."""
        self.client.enabled = False
        with patch('urllib.request.urlopen') as mock_urlopen:
            self.client.send_stats(100, 1.0)
            mock_urlopen.assert_not_called()
