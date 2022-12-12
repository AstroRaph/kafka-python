import json
from confluent_kafka import Producer

configuration = {'bootstrap.servers':'localhost:9092'}

producer = Producer(configuration)
print('Kafka Producer has been initiated...')

def main():
    #this data took for example
    data={
            "schema_ver": "0.1",
            "timestamp": "2022-12-09T12:35:12.8223086+03:00",
            "batch_id": "f466c39f-009a-4fb1-b962-bd7ffb20a113",
            "batch_size": 805,
            "sequence_num": 42,
            "source_name": "TIS_PORTAL",
            "message_type": "upsert",
            "payload": {
                "entity_type": "Unit",
                "company_name": None,
                "attributes": {
                    "unit_id": 59,
                    "parent_unit_id": 2,
                    "name": "TowerST",
                    "ordern": 455,
                    "short_name": "TowerST",
                    "unit_type": "COMPANY"
                }
            }
        }
    msg=json.dumps(data)
    producer.poll(1)
    producer.produce('tower', msg.encode('utf-8'))
    producer.flush()

if __name__ == '__main__':
    main()
