from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='192.168.99.100:29092', auto_offset_reset='earliest', consumer_timeout_ms=1000)

consumer.subscribe(['kafka-python-topic'])

for message in consumer:
    print(message)
    if self.stop_event.is_set():
        break