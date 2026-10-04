import argparse
import json

from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, COLLECTION_VALIDATED


def parse_args():
    parser = argparse.ArgumentParser(
        description="Aggregate orders by status."
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of statuses to return."
    )

    return parser.parse_args()


def run_aggregation(limit=20):
    client = MongoClient(MONGODB_URI)

    try:
        collection = client[DB_NAME][COLLECTION_VALIDATED]

        pipeline = [
            {
                "$group": {
                    "_id": "$status",
                    "order_count": {"$sum": 1},
                    "total_sales": {"$sum": "$total_amount"},
                    "average_order_value": {"$avg": "$total_amount"}
                }
            },
            {
                "$sort": {
                    "total_sales": -1
                }
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "_id": 0,
                    "status": "$_id",
                    "order_count": 1,
                    "total_sales": 1,
                    "average_order_value": 1
                }
            }
        ]

        data = list(collection.aggregate(pipeline))

        return {
            "report": "status_sales",
            "returned_count": len(data),
            "data": data
        }

    finally:
        client.close()


def main():
    args = parse_args()

    result = run_aggregation(args.limit)

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
