import argparse
import json

from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, COLLECTION_VALIDATED


def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze orders by city and status."
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of city-status groups to return."
    )

    return parser.parse_args()


def run_aggregation(limit=20):
    client = MongoClient(MONGODB_URI)

    try:
        collection = client[DB_NAME][COLLECTION_VALIDATED]

        pipeline = [
            {
                "$group": {
                    "_id": {
                        "city": "$city",
                        "status": "$status"
                    },
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
                    "city": "$_id.city",
                    "status": "$_id.status",
                    "order_count": 1,
                    "total_sales": 1,
                    "average_order_value": 1
                }
            }
        ]

        data = list(collection.aggregate(pipeline))

        return {
            "report": "city_status_analysis",
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
