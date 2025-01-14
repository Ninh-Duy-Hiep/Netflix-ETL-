# Tạo data giả 
``` python
import random
import time
import json

# Các giá trị mẫu
keywords = ["movie", "series", "comedy", "drama", "thriller", "action", "romantic", "titan", "school"]
account_types = ["free", "premium", "family", "student"]
devices = ["mobile", "tablet", "desktop", "smart_tv"]
locations = ["US", "UK", "India", "Vietnam", "Germany", "France"]
# movie_names = [
#     "Inception", "The Dark Knight", "Interstellar", "Parasite", "The Matrix",
#     "Breaking Bad", "The Crown", "Stranger Things", "The Witcher", "Money Heist"
# ]
movie_names = [f"Movie_{i}" for i in range(1, 101)] 
# Hàm tạo dữ liệu
def generate_data(num_records=1000):
    data = []
    for _ in range(num_records):
        record = {
            "keyword": random.choice(keywords),
            "search_time": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(random.randint(1609459200, 1672444800))),
            "user_id": random.randint(1000, 9999),
            "account_type": random.choice(account_types),
            "device": random.choice(devices),
            "location": random.choice(locations),
            "movie_name": random.choice(movie_names)
        }
        data.append(record)
    return data

# Lưu dữ liệu vào file JSON
data = generate_data(100)  
with open("mock_movie_data.json", "w") as file:
    json.dump(data, file, indent=4)

print("Dữ liệu giả đã được tạo thành công!")

```
# Sử dụng Kafka để thu thập thông tin 
**1. Khởi động Kafka :**
    - Khởi động zookeeper : 
        + `cd D:\kafka`
        + `bin\windows\zookeeper-server-start.bat config\zookeeper.properties`
    - Khởi động Kafka Broker :
        + `cd D:\kafka`
        + `bin\windows\kafka-server-start.bat config\server.properties`

**2. Tạo file `kafka_producer.py` :**
``` python
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
```

**3. Tạo file `kafka_consumer.py`:**
``` python
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
```
**4.Chạy đoạn mã `kafka_producer`**
**5.Chạy đoạn mã `kafka_consumer`**
**6.Để kiểm tra xem thông tin thu thập được chưa**
    - Lệnh để kiểm tra các topic đã tạo : `bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092`
    - Lệnh để kiểm tra các message trong topic (Trong trường hợp này các message trong topic movie-data là mình thu thập thông tin) : `bin\windows\kafka-console-consumer.bat --topic movie-data --from-beginning --bootstrap-server localhost:9092` . Chạy xong sẽ hiển thị 100 data giả được thu thập .
    - Lệnh để lấy 10 data thu thập được : `bin\windows\kafka-console-consumer.bat --topic movie-data --bootstrap-server localhost:9092 --max-messages 10 --from-beginning`


