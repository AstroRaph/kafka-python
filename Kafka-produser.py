import json
import random
import uuid
from datetime import datetime
import pytz

from confluent_kafka import Producer

configuration = {'bootstrap.servers':'localhost:9092'}

producer = Producer(configuration)
print('Kafka Producer has been initiated...')


def main():
    batch_id = uuid.uuid1()
    dt = datetime.now(pytz.timezone('Europe/Kyiv'))
    n = random.randint(100, 1000)
    m = random.randint(0, 100)
    data = {
            "schema_ver": "0.1",
            "timestamp": str(dt),
            "batch_id": str(batch_id),
            "batch_size": n,
            "sequence_num": m,
            "source_name": "TIS_PORTAL",
            "message_type": "upsert",
            "payload": {
                "entity_type": "Unit",
                "company_name": None,
                "attributes": {
                    "unit_id": m*2-1,
                    "parent_unit_id": 2,
                    "name": "TowerST",
                    "ordern": 455,
                    "short_name": "TowerST",
                    "unit_type": "COMPANY"
                }
            }
        }
    msg = json.dumps(data)
    producer.poll(1)
    producer.produce('tower', msg.encode('utf-8'))
    producer.flush()


if __name__ == '__main__':
    main()
