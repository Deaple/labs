from faker import Faker
import random
from datetime import datetime
import uuid
import json
import os
import string

# Initialize Faker
fake = Faker()

def generate_log_entry():
    data = {
        "data": fake.date_time_between(start_date="-1y", end_date="now", tzinfo=None).isoformat() + "Z",
        "correlationId": str(uuid.uuid4()),
        "timestamp": fake.date_time_between(start_date="-1y", end_date="now", tzinfo=None).replace(microsecond=0).isoformat() + "-03:00",
        "request": {
            "status": random.choice([200, 201, 301, 500, 403, 404]),
            "clientSSID": str(uuid.uuid4()),
            "connection": {
                "serial": random.randint(100000, 999999),
                "request_count": 1,
                "pipelined": random.choice([True, False]),
                "ssl": {
                    "protocol": "TLSv1.2",
                    "cipher": fake.random_element(elements=("ECDHE-RSA-AES256-GCM-SHA384", "AES256-SHA256")),
                    "session_id": str(uuid.uuid4()),
                    "session_reused": random.choice([True, False]),
                    "client_cert": {
                        "status": fake.random_element(elements=("SUCCESS", "FAILURE")),
                        "serial": fake.random_int(min=100000, max=999999),
                        "fingerprint": fake.uuid4(),
                        "subject": fake.company(),
                        "issuer": fake.company(),
                        "start": fake.date_time_between(start_date="-1y", end_date="now", tzinfo=None).replace(microsecond=0).strftime("%b %d %H:%M:%S:%f %Y GMT"),
                        "expires": fake.date_time_between(start_date="now", end_date="+1y", tzinfo=None).replace(microsecond=0).strftime("%b %d %H:%M:%S:%f %Y GMT"),
                        "expired": random.choice([True, False])
                    }
                },
                "remote_port": random.randint(1000, 9999),
                "serverOrgID": str(uuid.uuid4()),
                "path": fake.uri(),
                "http_version": 1.1,
                "headers": {
                    "user-agent": fake.user_agent(),
                    "backend": fake.domain_name(),
                    "x-enterprise-correlationid": str(uuid.uuid4()),
                    "host": fake.domain_name(),
                    "accept": "*/*",
                    "securityheader": "notxdetect-true",
                    "content-type": "application/json",
                    "x-client": str(uuid.uuid4()),
                    "x-fapi-interfaction-id": str(uuid.uuid4()),
                    "x-forward-for": fake.ipv4()
                },
                "duration": round(random.uniform(0.001, 5.000), 3),
                "contrato": fake.uri(),
                "consent": str(uuid.uuid4()),
                "bytes_received": random.randint(100, 2000),
                "remote_addr": fake.ipv4(),
                "serverASID": str(uuid.uuid4())
            },
            "response": {
                "status": random.choice([200, 201, 301, 500, 403, 404]),
                "bytes_sent": random.randint(100, 1000),
                "duration": round(random.uniform(0.001, 5.000), 3),
                "response_addr": fake.ipv4(),
                "headers": {
                    "Content-Type": "application/json",
                    "Content-Length": str(random.randint(50, 200)),
                    "x-amzn-RequestId": str(uuid.uuid4()),
                    "x-amz-apigw-id": str(uuid.uuid4()),
                    "X-Amzn-Trace-Id": "Root=" + str(uuid.uuid4())
                }
            }
        },
        "status": random.choice([200, 201, 301, 500, 403, 404]),
        "duration": round(random.uniform(0.1, 2.0), 3)
    }
    return data


def generate_fake_log_data():
    log_entry = {
        "timestamp": str(datetime.now()),
        "severity": random.choice(["INFO", "WARNING", "ERROR"]),
        "message": random.choice(["INFO", "WARNING", "ERROR"])
    }

    return log_entry

def save_log_to_file(log_data, file_path):
    with open(file_path, 'w') as file:
        json.dump(log_data, file, ensure_ascii=True)

def main():
    num_logs = 5
    output_directory = "data"
    logs = []
    for _ in range(num_logs):
        if random.choice([True, False]):
            logs.append(generate_log_entry())
        else:
            logs.append(generate_fake_log_data())
    print(logs)

    current_datetime = datetime.now()
    folder_path = os.path.join(output_directory, current_datetime.strftime("%Y/%m/%d"))
    os.makedirs(folder_path, exist_ok=True)
    file_name = str(uuid.uuid4()) + '.json'
    file_path = os.path.join(folder_path, file_name)
    save_log_to_file(logs, file_path)
    print(f"Log file saved: {file_path}")

if __name__ == "__main__":
    main()
