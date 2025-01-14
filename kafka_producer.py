from confluent_kafka import Producer
import json

# Cấu hình Kafka producer
conf = {
    'bootstrap.servers': 'localhost:9092',  # Địa chỉ Kafka server
    'client.id': 'movie-data-producer'
}

# Hàm callback để xử lý thành công hoặc lỗi khi gửi message
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Tạo Kafka producer
producer = Producer(conf)

# Đọc dữ liệu từ file JSON
with open("mock_movie_data.json", "r") as file:
    data = json.load(file)

# Gửi dữ liệu vào Kafka topic 'movie-data'
for record in data:
    producer.produce('movie-data', key=str(record["user_id"]), value=json.dumps(record), callback=delivery_report)

# Đảm bảo tất cả messages đã được gửi
producer.flush()
