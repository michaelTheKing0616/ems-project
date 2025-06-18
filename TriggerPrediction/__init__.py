import azure.functions as func
import requests
import json
import os
import psycopg2
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        endpoint_url = os.getenv("ENDPOINT_URL")
        api_key = os.getenv("API_KEY")

        input_data = {
            "data": [{
                "start": "2025-06-15T00:00:00Z",
                "target": [50.5] * 24,
                "feat_dynamic_real": [[22.3] * 24, [55.0] * 24, [1] * 24, [10.5] * 24, [60.0] * 24, [2300.0] * 24, [0.95] * 24, [220.0] * 24],
                "feat_static_cat": [0],
                "feat_static_real": [1000.0]
            }]
        }

        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
        response = requests.post(endpoint_url, json=input_data, headers=headers)
        response.raise_for_status()
        predictions = response.json()

        prediction = predictions["predictions"][0]["mean"]
        timestamp = datetime.strptime(predictions["predictions"][0]["start"], "%Y-%m-%dT%H:%M:%S%z")
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()
        cur.execute("INSERT INTO predictions (timestamp, prediction) VALUES (%s, %s)", (timestamp, json.dumps(prediction)))
        conn.commit()
        cur.close()
        conn.close()

        return func.HttpResponse("Predictions stored", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
