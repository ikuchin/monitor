import random
from unittest import TestCase

from website import WebsiteMock


class TestMsgProcessor(TestCase):
    def test_web_server(self):
        random.seed(0)
        web_server = WebsiteMock()
        web_server_status_codes = [web_server.get() for x in range(100)]
        expected = [
            (200, 'ok'), (200, 'ok'), (404, None), (404, None), (200, 'ok'),
            (404, None), (200, 'ok'), (200, 'ok'), (200, 'ok'), (200, 'error'),
            (500, None), (200, 'ok'), (500, None), (200, 'ok'), (200, 'error'),
            (200, 'ok'), (404, None), (200, 'ok'), (200, 'ok'), (500, None),
            (200, 'ok'), (404, None), (404, None), (404, None), (200, 'error'),
            (404, None), (200, 'error'), (200, 'error'), (500, None), (404, None),
            (200, 'ok'), (200, 'error'), (200, 'ok'), (200, 'ok'), (200, 'ok'),
            (404, None), (200, 'error'), (200, 'ok'), (404, None), (200, 'ok'),
            (404, None), (200, 'error'), (404, None), (200, 'ok'), (200, 'ok'),
            (404, None), (500, None), (200, 'ok'), (200, 'ok'), (200, 'ok'),
            (500, None), (200, 'ok'), (200, 'ok'), (200, 'error'), (500, None),
            (200, 'ok'), (200, 'error'), (200, 'error'), (200, 'error'), (404, None),
            (200, 'ok'), (200, 'ok'), (200, 'ok'), (500, None), (200, 'ok'),
            (200, 'ok'), (404, None), (200, 'error'), (200, 'ok'), (200, 'ok'),
            (500, None), (200, 'ok'), (200, 'error'), (200, 'error'), (200, 'ok'),
            (200, 'ok'), (200, 'error'), (200, 'error'), (200, 'error'), (500, None),
            (404, None), (500, None), (200, 'error'), (200, 'ok'), (500, None),
            (500, None), (200, 'ok'), (200, 'ok'), (200, 'error'), (500, None),
            (200, 'ok'), (200, 'error'), (404, None), (200, 'error'), (200, 'ok'),
            (200, 'ok'), (200, 'ok'), (200, 'ok'), (200, 'ok'), (200, 'ok')
        ]
        self.assertEqual(expected, web_server_status_codes)
