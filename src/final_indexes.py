from pymongo import ASCENDING

from config.settings import (
    MONGODB_URI,
    DB_NAME,
    COLLECTION_VALIDATED,
)
from pymongo import MongoClient


FINAL_INDEXES = [
    {
        "name": "uniq_order_id",
        "keys": [("order_id", ASCENDING)],
        "unique": True,
    },
    {
        "name": "idx_customer_id",
        "keys": [("customer_id", ASCENDING)],
    },
    {
        "name": "idx_city",
        "keys": [("city", ASCENDING)],
    },
    {
        "name": "idx_status_order_date",
        "keys": [
            ("status", ASCENDING),
            ("order_date", ASCENDING),
        ],
    },
]


def ensure_final_indexes():
    client = MongoClient(MONGODB_URI)

    try:
        db = client[DB_NAME]
        collection = db[COLLECTION_VALIDATED]

        created = []

        for index in FINAL_INDEXES:
            options = {
                "name": index["name"],
            }

            if index.get("unique"):
                options["unique"] = True

            collection.create_index(
                index["keys"],
                **options,
            )

            created.append(index["name"])

        return {
            "collection": COLLECTION_VALIDATED,
            "indexes": created,
            "count": len(created),
        }

    finally:
        client.close()


if __name__ == "__main__":
    print(ensure_final_indexes())
