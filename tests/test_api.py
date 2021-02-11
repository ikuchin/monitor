import random
from unittest import TestCase

from fastapi.testclient import TestClient

from api import app


class TestMsgProcessor(TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)
        app.extra["scheduler"].load_jobs()

    def test_api_root(self):
        response = self.client.get("/")
        assert response.status_code == 200
        self.assertDictContainsSubset(
            {
                "number_of_consumed_messages": 0,
                "number_of_produced_messages": 0,
                "number_of_running_jobs": 0,
                # 'scheduler_up_time': '0.01 second',
                "status": "ok",
            },
            response.json(),
        )

    def test_api_job_list(self):
        response = self.client.get("/job")
        assert response.status_code == 200
        self.assertEqual([{"1": "check localhost"}], response.json())

    def test_api_job_create(self):
        # ToDo: Create this test
        response = self.client.post(
            "/job",
            json={
                "check_period_seconds": 1,
                "uri": "http://localhost",
                "method": "get",
            },
        )
        assert response.status_code == 200
        self.assertEqual(
            {
                "job_id": 2,
                "job_name": 2,
                "check_period_seconds": 1,
                "stats": {
                    "counts_total": {},
                    "counts_by_hour": {},
                    "counts_by_minute": {},
                    "percent_total": {},
                },
                "number_of_send_messages": 0,
                "running": False,
            },
            response.json(),
        )

        response = self.client.get("/job")
        self.assertEqual([{"1": "check localhost"}, {"2": 2}], response.json())

    def test_api_job_info(self):
        response = self.client.get("/job/1")
        assert response.status_code == 200
        self.assertEqual(
            {
                "job_id": 1,
                "job_name": "check localhost",
                "check_period_seconds": 1,
                "stats": {
                    "counts_total": {},
                    "counts_by_hour": {},
                    "counts_by_minute": {},
                    "percent_total": {},
                },
                "number_of_send_messages": 0,
                "running": False,
            },
            response.json(),
        )

    def test_api_job_missing(self):
        response = self.client.get("/job/2")
        assert response.status_code == 200
        self.assertEqual({"error": "Job with job_id=2 not found"}, response.json())

    def test_api_job_start_stop(self):
        response = self.client.get("/job/1/start")
        assert response.status_code == 200

        response = self.client.get("/job/1")
        assert response.status_code == 200
        self.assertEqual(True, response.json()["running"])

        response = self.client.get("/job/1/stop")
        assert response.status_code == 200

        response = self.client.get("/job/1")
        assert response.status_code == 200
        self.assertEqual(False, response.json()["running"])

    def test_api_web_server(self):
        random.seed(0)
        response = self.client.get("/website_mock")
        assert response.status_code == 200
        assert response.content == b"ok"
