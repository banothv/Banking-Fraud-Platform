import json
import time

from kafka import KafkaProducer

from generator import generate_transaction


KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "bank-transactions"


def create_producer():

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )

    return producer


def main():

    print("=" * 70)
    print("BANKING TRANSACTION KAFKA PRODUCER")
    print("=" * 70)

    producer = create_producer()

    transaction_number = 1

    try:

        while True:

            transaction = generate_transaction(transaction_number)

            producer.send(
                KAFKA_TOPIC,
                value=transaction
            )

            producer.flush()

            print(
                f"Sent: {transaction['transaction_id']} | "
                f"Customer: {transaction['customer_id']} | "
                f"Amount: ₹{transaction['amount']} | "
                f"Location: {transaction['location']}"
            )

            transaction_number += 1

            time.sleep(2)

    except KeyboardInterrupt:

        print("\nStopping producer...")

    finally:

        producer.close()


if __name__ == "__main__":
    main()