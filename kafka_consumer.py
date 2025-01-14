from confluent_kafka import Consumer, KafkaException, KafkaError
import json

# Cấu hình Kafka consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'movie-data-consumer',
    'auto.offset.reset': 'earliest'  # Bắt đầu từ đầu nếu không có offset
}

# Tạo Kafka consumer
consumer = Consumer(conf)

# Đăng ký với topic 'movie-data'
consumer.subscribe(['movie-data'])

# Lấy dữ liệu từ Kafka topic
try:
    while True:
        msg = consumer.poll(timeout=1.0)  # Lấy message từ Kafka

        if msg is None:  # Nếu không có message mới
            continue
        if msg.error():  # Kiểm tra lỗi
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Đã đến cuối partition
                print(f"End of partition {msg.partition()} reached at offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            # Xử lý dữ liệu nhận được
            record = json.loads(msg.value().decode('utf-8'))
            print(f"Received record: {record}")

except KeyboardInterrupt:
    print("Consumer interrupted")

finally:
    # Đóng consumer
    consumer.close()
