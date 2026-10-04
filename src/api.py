from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder

from src.queries.query_01_customer import run_query as query_customer
from src.queries.query_02_city import run_query as query_city
from src.queries.query_03_status_date import run_query as query_status_date
from src.queries.query_04_order_id import run_query as query_order_id
from src.queries.query_05_date_range import run_query as query_date_range

from src.aggregations.aggregation_01_city_sales import run_aggregation as agg_city_sales
from src.aggregations.aggregation_02_status_sales import run_aggregation as agg_status_sales
from src.aggregations.aggregation_03_daily_sales import run_aggregation as agg_daily_sales
from src.aggregations.aggregation_04_top_customers import run_aggregation as agg_top_customers
from src.aggregations.aggregation_05_city_status import run_aggregation as agg_city_status

from src.materialized_views.mv_01_city_sales import refresh_city_sales
from src.materialized_views.mv_02_daily_sales import refresh_full as refresh_daily_sales

from src.jobs.job_runner import JOBS
from src.final_indexes import ensure_final_indexes
from src.scheduler import start_scheduler, stop_scheduler
from src.main import run_ingest


app = FastAPI(
    title="Big Data Final Project API",
    description="Unified API for the hybrid data pipeline",
    version="1.0.0",
)


QUERY_NAMES = [
    "customer",
    "city",
    "status_date",
    "order_id",
    "date_range",
]


AGGREGATION_NAMES = [
    "city_sales",
    "status_sales",
    "daily_sales",
    "top_customers",
    "city_status",
]


MV_NAMES = [
    "city_sales",
    "daily_sales",
]




@app.on_event("startup")
def startup_event():
    start_scheduler()


@app.on_event("shutdown")
def shutdown_event():
    stop_scheduler()

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "big-data-pipeline-api"
    }


@app.get("/queries")
def list_queries():
    return {
        "queries": QUERY_NAMES,
        "count": len(QUERY_NAMES)
    }


@app.get("/queries/{name}")
def execute_query(
    name: str,
    customer_id: str | None = None,
    city: str | None = None,
    status: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    order_id: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
):
    try:
        if name == "customer":
            if not customer_id:
                raise HTTPException(
                    status_code=400,
                    detail="customer_id is required"
                )
            result = query_customer(customer_id, limit)

        elif name == "city":
            if not city:
                raise HTTPException(
                    status_code=400,
                    detail="city is required"
                )
            result = query_city(city, limit)

        elif name == "status_date":
            if not status or not start_date or not end_date:
                raise HTTPException(
                    status_code=400,
                    detail="status, start_date and end_date are required"
                )
            result = query_status_date(
                status,
                start_date,
                end_date,
                limit
            )

        elif name == "order_id":
            if not order_id:
                raise HTTPException(
                    status_code=400,
                    detail="order_id is required"
                )
            result = query_order_id(order_id)

        elif name == "date_range":
            if not start_date or not end_date:
                raise HTTPException(
                    status_code=400,
                    detail="start_date and end_date are required"
                )
            result = query_date_range(
                start_date,
                end_date,
                limit
            )

        else:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown query: {name}"
            )

        return jsonable_encoder({
            "query": name,
            "result": result
        })

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )


@app.get("/aggregations")
def list_aggregations():
    return {
        "aggregations": AGGREGATION_NAMES,
        "count": len(AGGREGATION_NAMES)
    }


@app.get("/aggregations/{name}")
def execute_aggregation(
    name: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
):
    try:
        if name == "city_sales":
            result = agg_city_sales(limit)

        elif name == "status_sales":
            result = agg_status_sales(limit)

        elif name == "daily_sales":
            result = agg_daily_sales(limit)

        elif name == "top_customers":
            if not start_date or not end_date:
                raise HTTPException(
                    status_code=400,
                    detail="start_date and end_date are required"
                )
            result = agg_top_customers(
                start_date,
                end_date,
                limit
            )

        elif name == "city_status":
            result = agg_city_status(limit)

        else:
            raise HTTPException(
                status_code=404,
                detail=f"Unknown aggregation: {name}"
            )

        return jsonable_encoder({
            "aggregation": name,
            "result": result
        })

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )




@app.post("/ingest")
def ingest(
    input_path: str,
    threshold_mb: float | None = None,
    batch_size: int | None = None,
    stage: str = "normal",
):
    if stage not in {"normal", "initial", "delta"}:
        raise HTTPException(
            status_code=400,
            detail="stage must be normal, initial, or delta"
        )

    if not input_path:
        raise HTTPException(
            status_code=400,
            detail="input_path is required"
        )

    try:
        result = run_ingest(
            input_path=input_path,
            threshold_mb=threshold_mb,
            batch_size=batch_size,
            stage=stage,
        )

        return jsonable_encoder({
            "status": "success",
            "result": result
        })

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )


@app.post("/indexes")
def create_indexes():
    try:
        result = ensure_final_indexes()

        return jsonable_encoder({
            "status": "success",
            "result": result
        })

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )


@app.post("/refresh-mv")
def refresh_materialized_view(
    name: str = Query(...),
    full_refresh: bool = Query(default=False),
):
    if name not in MV_NAMES:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown materialized view: {name}"
        )

    try:
        if name == "city_sales":
            result = refresh_city_sales(
                full_refresh=full_refresh
            )

        else:
            from pymongo import MongoClient
            from config.settings import MONGODB_URI, DB_NAME

            client = MongoClient(MONGODB_URI)

            try:
                db = client[DB_NAME]

                if full_refresh:
                    result = refresh_daily_sales(db)
                else:
                    from src.materialized_views.mv_02_daily_sales import refresh_incremental
                    result = refresh_incremental(db)

            finally:
                client.close()

        return jsonable_encoder({
            "materialized_view": name,
            "result": result
        })

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )


@app.get("/jobs")
def list_jobs():
    return {
        "jobs": list(JOBS.keys()),
        "count": len(JOBS)
    }


@app.post("/jobs/{name}/run")
def run_job(name: str):
    if name not in JOBS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown job: {name}"
        )

    try:
        result = JOBS[name]()

        return jsonable_encoder({
            "job": name,
            "status": "success",
            "result": result
        })

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc)
        )
