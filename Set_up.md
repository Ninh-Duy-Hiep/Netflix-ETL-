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
1. Khởi động Kafka :
