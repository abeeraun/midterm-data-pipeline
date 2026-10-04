from apscheduler.schedulers.background import BackgroundScheduler

from src.jobs.job_runner import (
    run_city_sales_job,
    run_daily_sales_job,
)


scheduler = BackgroundScheduler()


def start_scheduler():
    if scheduler.running:
        return

    scheduler.add_job(
        run_city_sales_job,
        trigger="interval",
        hours=1,
        id="refresh_city_sales",
        replace_existing=True,
    )

    scheduler.add_job(
        run_daily_sales_job,
        trigger="interval",
        hours=1,
        id="refresh_daily_sales",
        replace_existing=True,
    )

    scheduler.start()

    print("[SCHEDULER] started")
    print("[SCHEDULER] refresh_city_sales: every 1 hour")
    print("[SCHEDULER] refresh_daily_sales: every 1 hour")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        print("[SCHEDULER] stopped")


if __name__ == "__main__":
    start_scheduler()

    try:
        import time

        while True:
            time.sleep(60)

    except (KeyboardInterrupt, SystemExit):
        stop_scheduler()
