from datetime import datetime, timezone
from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME
from src.materialized_views.mv_01_city_sales import refresh_city_sales
from src.materialized_views.mv_02_daily_sales import refresh_incremental as refresh_daily_sales


def run_city_sales_job():
    started_at = datetime.now(timezone.utc)

    print(f"[JOB START] refresh_city_sales | {started_at.isoformat()}")

    try:
        result = refresh_city_sales(full_refresh=False)

        status = "success"
        error = None

    except Exception as exc:
        result = None
        status = "failed"
        error = str(exc)

    finished_at = datetime.now(timezone.utc)

    client = MongoClient(MONGODB_URI)

    try:
        db = client[DB_NAME]

        log = {
            "job_name": "refresh_city_sales",
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "result": result,
            "error": error
        }

        db.job_logs.insert_one(log)

    finally:
        client.close()

    print(f"[JOB END] refresh_city_sales | status={status}")

    if error:
        print(f"Error: {error}")

    return log


def run_daily_sales_job():
    started_at = datetime.now(timezone.utc)

    print(f"[JOB START] refresh_daily_sales | {started_at.isoformat()}")

    client = MongoClient(MONGODB_URI)

    try:
        db = client[DB_NAME]

        result = refresh_daily_sales(db)

        status = "success"
        error = None

    except Exception as exc:
        result = None
        status = "failed"
        error = str(exc)

    finally:
        client.close()

    finished_at = datetime.now(timezone.utc)

    client = MongoClient(MONGODB_URI)

    try:
        db = client[DB_NAME]

        log = {
            "job_name": "refresh_daily_sales",
            "started_at": started_at,
            "finished_at": finished_at,
            "status": status,
            "result": result,
            "error": error
        }

        db.job_logs.insert_one(log)

    finally:
        client.close()

    print(f"[JOB END] refresh_daily_sales | status={status}")

    if error:
        print(f"Error: {error}")

    return log


JOBS = {
    "refresh_city_sales": run_city_sales_job,
    "refresh_daily_sales": run_daily_sales_job,
}

