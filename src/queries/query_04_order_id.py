import argparse
import json

from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, COLLECTION_VALIDATED


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find a validated order by order_id."
    )

    parser.add_argument(
        "--order-id",
        required=True,
        help="Order ID to search for."
    )

    return parser.parse_args()


def run_query(order_id):
    client = MongoClient(MONGODB_URI)

    try:
        collection = client[DB_NAME][COLLECTION_VALIDATED]

        document = collection.find_one(
            {"order_id": order_id},
            {
                "_id": 0,
                "order_id": 1,
                "order_date": 1,
                "status": 1,
                "customer_id": 1,
                "city": 1,
                "total_amount": 1
            }
        )

        return {
            "query": "order_id_lookup",
            "order_id": order_id,
            "found": document is not None,
            "data": document
        }

    finally:
        client.close()


def main():
    args = parse_args()

    result = run_query(args.order_id)

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
