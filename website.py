import random


class WebsiteMock:
    correct_content = "ok"
    invalid_content = "error"

    def get(self):
        i = random.randint(1, 10)
        if i <= 7:
            status_code = 200  # 70% Should return 200
        elif i <= 9:
            status_code = 404  # 20% Should return 200
        else:
            status_code = 500  # 10% Should return 200

        if status_code == 200:
            if random.randint(1, 10) > 3:
                content = self.correct_content  # 70% Should return correct answer
            else:
                content = self.invalid_content  # 30% Should return invalid answer
        else:
            content = None

        return status_code, content
