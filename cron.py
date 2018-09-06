from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import sys
from smyt_sync_manager import task


def quote_send_sh_job():
    print('task start at ', datetime.datetime.now())
    task.execute_from_commandline()

if __name__ == '__main__':
    print(sys.argv)
    scheduler = BlockingScheduler()
    if len(sys.argv) == 6:
        params = sys.argv[1:]
        scheduler.add_job(quote_send_sh_job, 'cron', minute=params[0], hour=params[1], day=params[2], month=params[3], day_of_week=params[4])
        print("scheduler running! crontab: {}".format(" ".join(params)))
    else:
        scheduler.add_job(quote_send_sh_job, 'cron', hour='20') #每天20点启动
        print("param error! user default crontab: 0 20 * * *")
    scheduler.start()