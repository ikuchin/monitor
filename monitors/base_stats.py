from collections import defaultdict
from datetime import datetime


class BaseStats:
    def __init__(self):
        self.counts_total = defaultdict(int)
        self.counts_by_hour = defaultdict(lambda: defaultdict(int))
        self.counts_by_minute = defaultdict(lambda: defaultdict(int))

    def update_counter(self, dt: datetime, key):
        self.counts_total[key] += 1
        self.counts_by_hour[dt.replace(minute=0, second=0, microsecond=0).isoformat()][key] += 1
        self.counts_by_minute[dt.replace(second=0, microsecond=0).isoformat()][key] += 1

    def dict(self):
        total_count_sum = sum(self.counts_total.values())
        return {
            "counts_total": self.counts_total,
            "counts_by_hour": self.counts_by_hour,
            "counts_by_minute": self.counts_by_minute,
            "percent_total": {
                k: f"{100 / total_count_sum * v:.2f}%"
                for k, v in self.counts_total.items()
            }
        }