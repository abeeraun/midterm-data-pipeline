import argparse
import json
from datetime import datetime, timezone

from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, COLLECTION_VALIDATED


MV_COLLECTION = "mv_city_sales"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Refresh the city sales materialized view."
    )

    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Rebuild the materialized view from all validated orders."
    )

    return parser.parse_args()


def get_last_refresh(collection):
    document = collection.find_one(
        {},
        {"_id": 0, "last_source_date": 1},
        sort=[("last_source_date", -1)]
    )

    if document and document.get("last_source_date"):
        return document["last_source_date"]

    return None


def refresh_city_sales(full_refresh=False):
    client = MongoClient(MONGODB_URI)

    try:
        source = client[DB_NAME][COLLECTION_VALIDATED]
        target = client[DB_NAME][MV_COLLECTION]

        if full_refresh:
            target.delete_many({})

        last_source_date = None if full_refresh else get_last_refresh(target)

        match_stage = {}

        if last_source_date is not None:
            match_stage = {
                "order_date": {
                    "$gt": last_source_date
                }
            }

        pipeline = []

        if match_stage:
            pipeline.append({"$match": match_stage})

        pipeline.extend(
            [
                {
                    "$group": {
                        "_id": "$city",
                        "order_count": {"$sum": 1},
                        "total_sales": {"$sum": "$total_amount"}
                    }
                }
            ]
        )

        incremental_data = list(source.aggregate(pipeline))

        refresh_time = datetime.now(timezone.utc)

        for item in incremental_data:
            city = item["_id"]

            target.update_one(
                {"city": city},
                {
                    "$inc": {
                        "order_count": item["order_count"],
                        "total_sales": item["total_sales"]
                    },
                    "$set": {
                        "last_refresh": refresh_time
                    }
                },
                upsert=True
            )

        for document in target.find({}):
            if document.get("order_count", 0) > 0:
                target.update_one(
                    {"_id": document["_id"]},
                    [
                        {
                            "$set": {
                                "average_order_value": {
                                    "$divide": [
                                        "$total_sales",
                                        "$order_count"
                                    ]
                                }
                            }
                        }
                    ]
                )

        latest_source = source.find_one(
            {"order_date": {"$ne": None}},
            {"_id": 0, "order_date": 1},
            sort=[("order_date", -1)]
        )

        if latest_source:
            target.update_many(
                {},
                {
                    "$set": {
                        "last_source_date": latest_source["order_date"],
                        "last_refresh": refresh_time
                    }
                }
            )

        return {
            "materialized_view": MV_COLLECTION,
            "mode": "full" if full_refresh else "incremental",
            "processed_groups": len(incremental_data),
            "last_source_date": (
                latest_source["order_date"]
                if latest_source
                else None
            ),
            "last_refresh": refresh_time
        }

    finally:
        client.close()


def main():
    args = parse_args()

    result = refresh_city_sales(
        full_refresh=args.full_refresh
    )

    print(
        json.dumps(
            result,
            ensure_ascii=False,
            default=str,
            indent=2
        )
    )


if __name__ == "__main__":
    main()

