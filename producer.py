import csv
import json
import time
from datetime import datetime, UTC
from kafka import KafkaProducer

print("PaySim Producer starting...", flush=True)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Kafka producer created", flush=True)

csv_file_path = "/app/data/paysim.csv"

while True:
    try:
        print("Opening PaySim CSV...", flush=True)

        with open(csv_file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                payload = {
                    "transaction_id": f"{row['nameOrig']}_{row['nameDest']}_{row['step']}",
                    "simulation_step": int(row["step"]),
                    "event_time": datetime.now(UTC).isoformat(),
                    "transaction_type": row["type"],
                    "amount": float(row["amount"]),
                    "source_account": row["nameOrig"],
                    "destination_account": row["nameDest"],
                    "old_balance_origin": float(row["oldbalanceOrg"]),
                    "new_balance_origin": float(row["newbalanceOrig"]),
                    "old_balance_dest": float(row["oldbalanceDest"]),
                    "new_balance_dest": float(row["newbalanceDest"]),
                    "fraud_label": int(row["isFraud"]),
                    "flagged_fraud": int(row["isFlaggedFraud"])
                }

                producer.send("transactions", value=payload)
                producer.flush()

                print(f"Sent transaction: {payload}", flush=True)
                time.sleep(2)

        print("Reached end of CSV, restarting from beginning...", flush=True)
        time.sleep(5)

    except Exception as e:
        print(f"Error: {e}", flush=True)
        time.sleep(5)
