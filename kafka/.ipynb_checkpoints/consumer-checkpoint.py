from confluent_kafka import Consumer
from confluent_kafka.serializer import SerializerError


c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup2',
    'auto.offset.rest':'earliest'
})

c.subscribe(['greetings'])

while True:
    
    msg = c.poll(10)



    if msg is None:
        continue

    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('received msg: {}'.format(msg.value().decode('utf-8')))

c.close()