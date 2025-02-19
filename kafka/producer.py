from confluent_kafka import Producer
import json

# Cấu hình Producer Kafka
conf = {
    'bootstrap.servers':'10.6.98.110:9092',  # Địa chỉ máy chủ Kafka trong LAN
    'client.id': 'movie-data-producer'
}

# Hàm gửi dữ liệu vào Kafka
def send_data_to_kafka(data):
    producer = Producer(conf)
    
    for record in data:
        # Chuyển đổi dữ liệu thành JSON và gửi vào Kafka
        producer.produce('movie_data', key=str(record["user_id"]), value=json.dumps(record))
        producer.flush()

# Đọc dữ liệu từ file JSON và gửi đến Kafka
with open('./data/mock_movie_data.json', 'r') as file:
    data = json.load(file)

send_data_to_kafka(data)
print("Dữ liệu đã được gửi vào Kafka.")
