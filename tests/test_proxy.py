import unittest
import os
from unittest.mock import patch, MagicMock

import apps.webhook.main as main_module
import cleanarr.webhook.proxy as proxy_module

class TestProxy(unittest.TestCase):
    @patch("apps.webhook.main.run_proxy")
    @patch("apps.webhook.main._start_background_threads")
    @patch("apps.webhook.main.APP")
    def test_proxy_mode_enabled(self, mock_app, mock_threads, mock_run_proxy):
        with patch.dict(os.environ, {"CLEANARR_WEBHOOK_FORWARD_URL": "http://lambda"}):
            main_module.main()
            mock_run_proxy.assert_called_once()
            mock_app.run.assert_not_called()
            mock_threads.assert_not_called()

    @patch("apps.webhook.main.run_proxy")
    @patch("apps.webhook.main._start_background_threads")
    @patch("apps.webhook.main.APP")
    def test_direct_mode_enabled(self, mock_app, mock_threads, mock_run_proxy):
        with patch.dict(os.environ, {"CLEANARR_WEBHOOK_FORWARD_URL": ""}):
            main_module.main()
            mock_run_proxy.assert_not_called()
            mock_app.run.assert_called_once()
            mock_threads.assert_called_once()

if __name__ == '__main__':
    unittest.main()
