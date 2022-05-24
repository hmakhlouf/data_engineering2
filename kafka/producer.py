from confluent_kafka import Producer

# pip install confluent-kafka
#     kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic greetings

# open terminal
# cd workshop
# cd kafka
# python producer.py



p = Producer({'bootstrap.servers': 'localhost:9092'})

# delivery_report is called after message delivered to broker
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}] offset {}'.format(msg.topic(), msg.partition(), msg.offset()))

for data in ['hello world', 'good afternoon']:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce('greetings', data.encode('utf-8'), callback=delivery_report)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()