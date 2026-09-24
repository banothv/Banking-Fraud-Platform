import json
import random
import time
from datetime import datetime

from faker import Faker


fake = Faker("en_IN")


CUSTOMERS = [
    {
        "customer_id": "CUST001",
        "name": "Banoth Venkatesh ",
        "normal_location": "Hyderabad"
    },
    {
        "customer_id": "CUST002",
        "name": "Katta Prasad",
        "normal_location": "Bangalore"
    },
    {
        "customer_id": "CUST003",
        "name": "Banoth mahesh",
        "normal_location": "Mumbai"
    },
    {
        "customer_id": "CUST004",
        "name": "Dumpala Madhu",
        "normal_location": "Delhi"
    },
    {
        "customer_id": "CUST005",
        "name": "Kurmai Madhav",
        "normal_location": "Chennai"
    }
]


MERCHANTS = [
    "Amazon",
    "Flipkart",
    "Walmart",
    "Swiggy",
    "Zomato",
    "Uber",
    "BookMyShow",
    "Apple Store",
    "Google Store"
]


LOCATIONS = [
    "Hyderabad",
    "Mumbai",
    "Delhi",
    "Bangalore",
    "Chennai",
    "Pune",
    "Kolkata"
]


PAYMENT_METHODS = [
    "UPI",
    "CARD",
    "NET_BANKING",
    "WALLET"
]


def generate_transaction(transaction_number):
    """
    Generate one fake banking transaction.
    """

    customer = random.choice(CUSTOMERS)

    transaction = {
        "transaction_id": f"TXN{transaction_number:08d}",
        "customer_id": customer["customer_id"],
        "customer_name": customer["name"],
        "amount": round(random.uniform(100, 100000), 2),
        "currency": "INR",
        "merchant": random.choice(MERCHANTS),
        "location": random.choice(LOCATIONS),
        "normal_location": customer["normal_location"],
        "payment_method": random.choice(PAYMENT_METHODS),
        "transaction_time": datetime.now().isoformat()
    }

    return transaction


def main():

    print("=" * 70)
    print("REAL-TIME BANKING TRANSACTION GENERATOR")
    print("=" * 70)

    transaction_number = 1

    while True:

        transaction = generate_transaction(transaction_number)

        print(json.dumps(transaction, indent=4))

        print("-" * 70)

        transaction_number += 1

        time.sleep(2)


if __name__ == "__main__":
    main()