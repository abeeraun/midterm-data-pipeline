import argparse
import json
from datetime import datetime

from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, COLLECTION_VALIDATED


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find top customers within a date range."
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format."
    )

    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format."
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of top customers to return."
    )

    return parser.parse_args()


def parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d")


def run_aggregation(start_date, end_date, limit=20):
    client = MongoClient(MONGODB_URI)

    try:
        collection = client[DB_NAME][COLLECTION_VALIDATED]

        pipeline = [
            {
                "$match": {
                    "order_date": {
                        "$gte": parse_date(start_date),
                        "$lt": parse_date(end_date)
                    }
                }
            },
            {
                "$group": {
                    "_id": "$customer_id",
                    "order_count": {"$sum": 1},
                    "total_spent": {"$sum": "$total_amount"},
                    "average_order_value": {"$avg": "$total_amount"}
                }
            },
            {
                "$sort": {
                    "total_spent": -1
                }
            },
            {
                "$limit": limit
            },
            {
                "$project": {
                    "_id": 0,
                    "customer_id": "$_id",
                    "order_count": 1,
                    "total_spent": 1,
                    "average_order_value": 1
                }
            }
        ]

        data = list(collection.aggregate(pipeline))

        return {
            "report": "top_customers",
            "start_date": start_date,
            "end_date": end_date,
            "returned_count": len(data),
            "data": data
        }

    finally:
        client.close()


def main():
    args = parse_args()

    result = run_aggregation(
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit
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
