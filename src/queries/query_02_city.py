import argparse
import json
from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, COLLECTION_VALIDATED


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find validated orders by city."
    )
    parser.add_argument(
        "--city",
        required=True,
        help="City to search for."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of orders to return."
    )
    return parser.parse_args()


def run_query(city, limit=20):
    client = MongoClient(MONGODB_URI)

    try:
        collection = client[DB_NAME][COLLECTION_VALIDATED]

        cursor = collection.find(
            {"city": city},
            {
                "_id": 0,
                "order_id": 1,
                "customer_id": 1,
                "city": 1,
                "status": 1,
                "order_date": 1,
                "total_amount": 1
            }
        ).limit(limit)

        data = list(cursor)

        return {
            "query": "city_lookup",
            "city": city,
            "returned_count": len(data),
            "data": data
        }

    finally:
        client.close()


def main():
    args = parse_args()

    result = run_query(
        city=args.city,
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
