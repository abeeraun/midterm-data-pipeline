import argparse
import json
from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, COLLECTION_VALIDATED


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find validated orders by customer_id."
    )
    parser.add_argument(
        "--customer-id",
        required=True,
        help="Customer ID to search for."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of orders to return."
    )
    return parser.parse_args()


def run_query(customer_id, limit=20):
    client = MongoClient(MONGODB_URI)

    try:
        collection = client[DB_NAME][COLLECTION_VALIDATED]

        cursor = collection.find(
            {"customer_id": customer_id},
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
            "query": "customer_lookup",
            "customer_id": customer_id,
            "returned_count": len(data),
            "data": data
        }

    finally:
        client.close()


def main():
    args = parse_args()

    result = run_query(
        customer_id=args.customer_id,
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
