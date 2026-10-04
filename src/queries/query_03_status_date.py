import argparse
import json
from datetime import datetime

from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, COLLECTION_VALIDATED


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find validated orders by status and date range."
    )

    parser.add_argument(
        "--status",
        required=True,
        help="Order status to search for."
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
        help="Maximum number of orders to return."
    )

    return parser.parse_args()


def parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d")


def run_query(status, start_date, end_date, limit=20):
    client = MongoClient(MONGODB_URI)

    try:
        collection = client[DB_NAME][COLLECTION_VALIDATED]

        query = {
            "status": status,
            "order_date": {
                "$gte": parse_date(start_date),
                "$lt": parse_date(end_date)
            }
        }

        cursor = collection.find(
            query,
            {
                "_id": 0,
                "order_id": 1,
                "order_date": 1,
                "status": 1,
                "customer_id": 1,
                "city": 1,
                "total_amount": 1
            }
        ).limit(limit)

        data = list(cursor)

        return {
            "query": "status_date_range",
            "status": status,
            "start_date": start_date,
            "end_date": end_date,
            "returned_count": len(data),
            "data": data
        }

    finally:
        client.close()


def main():
    args = parse_args()

    result = run_query(
        status=args.status,
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
