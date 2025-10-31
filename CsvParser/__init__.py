import logging
import azure.functions as func
import csv
import io
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        content = req.get_body().decode('utf-8')
        reader = csv.DictReader(io.StringIO(content))

        documents = []
        for row in reader:
            doc = {
                "id": f"row-{row['timestamp']}-{row['machine_id']}",
                "timestamp": row["timestamp"],
                "machine_id": row["machine_id"],
                "temperature": float(row["temperature"]),
                "vibration": float(row["vibration"]),
                "humidity": float(row["humidity"]),
                "pressure": float(row["pressure"]),
                "energy_consumption": float(row["energy_consumption"]),
                "machine_status": row["machine_status"],
                "anomaly_flag": row["anomaly_flag"],
                "predicted_remaining_life": int(row["predicted_remaining_life"]),
                "failure_type": row["failure_type"],
                "downtime_risk": float(row["downtime_risk"]),
                "maintenance_required": row["maintenance_required"],
                "text_representation": (
                    f"At {row['timestamp']}, Machine {row['machine_id']} recorded "
                    f"temperature {row['temperature']}, vibration {row['vibration']}, "
                    f"humidity {row['humidity']}, pressure {row['pressure']}, "
                    f"energy consumption {row['energy_consumption']}. "
                    f"Status {row['machine_status']}, anomaly flag {row['anomaly_flag']}, "
                    f"predicted remaining life {row['predicted_remaining_life']} hours, "
                    f"failure type {row['failure_type']}, downtime risk {row['downtime_risk']}, "
                    f"maintenance required {row['maintenance_required']}."
                )
            }
            documents.append(doc)

        return func.HttpResponse(json.dumps(documents), mimetype="application/json")

    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse("Failed to parse CSV", status_code=500)
