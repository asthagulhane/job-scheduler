from locust import HttpUser, task, between

class TaskForgeUser(HttpUser):
    wait_time = between(0.5, 2)

    @task
    def submit_job(self):
        self.client.post("/jobs", json={
            "payload": {"task": "load_test", "source": "locust"}
        })