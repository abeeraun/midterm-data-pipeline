import argparse
from datetime import datetime, timezone

from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, COLLECTION_VALIDATED


MV_COLLECTION = "mv_daily_sales"


def get_client():
    return MongoClient(MONGODB_URI)


def refresh_full(db):
    source = db[COLLECTION_VALIDATED]
    target = db[MV_COLLECTION]

    target.delete_many({})

    pipeline = [
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$order_date"
                    }
                },
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"},
                "average_order_value": {"$avg": "$total_amount"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "order_date": "$_id",
                "order_count": 1,
                "total_sales": 1,
                "average_order_value": 1
            }
        },
        {
            "$sort": {
                "order_date": 1
            }
        }
    ]

    results = list(source.aggregate(pipeline, allowDiskUse=True))

    if results:
        target.insert_many(results)

    last_date = results[-1]["order_date"] if results else None

    target.update_one(
        {"_metadata": "refresh_state"},
        {
            "$set": {
                "_metadata": "refresh_state",
                "last_source_date": last_date,
                "last_refresh": datetime.now(timezone.utc)
            }
        },
        upsert=True
    )

    return {
        "materialized_view": MV_COLLECTION,
        "mode": "full",
        "processed_groups": len(results),
        "last_source_date": last_date
    }


def refresh_incremental(db):
    source = db[COLLECTION_VALIDATED]
    target = db[MV_COLLECTION]

    state = target.find_one({"_metadata": "refresh_state"})

    if not state or not state.get("last_source_date"):
        return refresh_full(db)

    last_date = state["last_source_date"]

    pipeline = [
        {
            "$match": {
                "order_date": {
                    "$gt": datetime.strptime(last_date, "%Y-%m-%d")
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$order_date"
                    }
                },
                "order_count": {"$sum": 1},
                "total_sales": {"$sum": "$total_amount"},
                "average_order_value": {"$avg": "$total_amount"}
            }
        }
    ]

    results = list(source.aggregate(pipeline, allowDiskUse=True))

    for item in results:
        target.update_one(
            {"order_date": item["_id"]},
            {
                "$set": {
                    "order_count": item["order_count"],
                    "total_sales": item["total_sales"],
                    "average_order_value": item["average_order_value"]
                }
            },
            upsert=True
        )

    new_last_date = last_date

    if results:
        new_last_date = max(item["_id"] for item in results)

    target.update_one(
        {"_metadata": "refresh_state"},
        {
            "$set": {
                "_metadata": "refresh_state",
                "last_source_date": new_last_date,
                "last_refresh": datetime.now(timezone.utc)
            }
        },
        upsert=True
    )

    return {
        "materialized_view": MV_COLLECTION,
        "mode": "incremental",
        "processed_groups": len(results),
        "last_source_date": new_last_date
    }


def main():
    parser = argparse.ArgumentParser(
        description="Refresh daily sales materialized view"
    )

    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Rebuild the materialized view from the source collection"
    )

    args = parser.parse_args()

    client = get_client()

    try:
        db = client[DB_NAME]

        if args.full_refresh:
            result = refresh_full(db)
        else:
            result = refresh_incremental(db)

        print(result)

    finally:
        client.close()


if __name__ == "__main__":
    main()
