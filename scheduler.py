import schedule
import time
from os import system


def job():
    system('main.py')


schedule.every(1).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
