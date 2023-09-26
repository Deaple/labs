import json
import os
import random
import string
import uuid
from datetime import datetime
from faker import Faker

fake = Faker()

def generate_log_entry():
    log_entry = {
        "date": fake.date_time_this_decade().isoformat(),
        "correlationId": str(uuid.uuid4()),
        "timestamp": fake.date_time_this_decade().isoformat(),
        "timestampwithms": fake.date_time_this_decade().isoformat(),
        "request": {
            "status": random.choice([200, 201, 301, 500, 403, 404]),
            "connection": {
                "serial": random.randint(1, 100),
                "request_count": 1,
                "pipelined": random.choice([True, False]),
                "ssl": {
                    "protocol": "TLSv1.2",
                    "cipher": fake.random_element(elements=("ECDHE-RSA-AES256-GCM-SHA384", "AES256-SHA256")),
                    "session_id": str(uuid.uuid4()),
                    "session_reused": random.choice([True, False]),
                    "zero_rtt": random.choice([True, False]),
                    "client_cert": {
                        "status": fake.random_element(elements=("SUCCESS", "FAILURE")),
                        "serial": fake.uuid4(),
                        "fingerprint": fake.word(),
                        "subject": fake.word(),
                        "issuer": fake.word(),
                        "starts": fake.date_time_between(start_date="-1y", end_date="now", tzinfo=None).replace(microsecond=0).strftime("%b %d %H:%M:%S:%f %Y GMT"),
                        "expires": fake.date_time_between(start_date="now", end_date="+1y", tzinfo=None).replace(microsecond=0).strftime("%b %d %H:%M:%S:%f %Y GMT"),
                        "expired": random.choice([True, False])
                    }
                }
            },
            "remote_port": random.randint(1024, 65535),
            "path": fake.uri(),
            "http_version": "1.1",
            "headers": {
                "user-agent": fake.user_agent(),
                "x-person-type": "person-type",
                "x-b3-parentspanid": str(uuid.uuid4()),
                "x-forward-header-names": fake.domain_name(),
                "x-request-id": str(uuid.uuid4()),
                "x-request-device-id": str(uuid.uuid4()),
                "host": fake.domain_name(),
                "x-trace-digest-69": fake.md5(),
                "x-api-source": fake.domain_name(),
                "tracerparent": "traceparent-id-value",
                "x-source-ip": fake.ipv4(),
                "x-fapi-interaction-id": str(uuid.uuid4()),
                "x-org-id": "org-id-value",
                "x-api-authentication-v2-present": "",
                "content-length": random.randint(1, 100),
                "x-b3-spanid": str(uuid.uuid4()),
                "x-api-transport-protocol": "tls",
                "x-consent-id": "consent-" + str(uuid.uuid4()),
                "x-pcm-custom-field": str(uuid.uuid4())
            },
            "duration": round(random.uniform(0.1, 2.0), 3),
            "contrato": fake.uri(),
            "bytes_received": random.randint(1000, 5000),
            "brand-id": fake.word()
        },
        "response": {
            "status": random.choice([200, 201, 301, 500, 403, 404]),
            "bytes_sent": random.randint(1000, 5000),
            "duration": round(random.uniform(0.1, 2.0), 3),
            "response_addr": fake.ipv4(),
            "headers": {
                "x-fapi-interaction-id": str(uuid.uuid4()),
                "Content-Type": "application/json",
                "Content-Length": str(random.randint(100, 1000))
            }
        },
        "status": random.choice([200, 201, 301, 500, 403, 404]),
        "duration": round(random.uniform(0.1, 2.0), 3)
    }
    return log_entry

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
