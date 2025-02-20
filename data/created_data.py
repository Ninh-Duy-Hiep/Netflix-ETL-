import random
import time
import json

# Các giá trị mẫu
# keywords = ["   movie love  ", "@@@series", "Come@dy", "draMak", "Thriller", "ACTION12", "rooomantic", "TItaN", "school", "12345", "a"]
# account_types = ["free", "premium", "family", "student"]
# devices = ["mobile", "tablet", "desktop", "smart_tv"]
# locations = ["US", "UK", "India", "Vietnam", "Germany", "France"]

keywords = ["king dom", "hello boy" , "hello girl"]
account_types = ["free", "premium", "family"]
devices = ["mobile", "tablet", "desktop"]
locations = ["US", "UK", "India"]
movie_names = [f"Movie_{i}" for i in range(1, 11)] 
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
data = generate_data(10)  
with open("./data/mock_movie_data_small2.json", "w") as file:
    json.dump(data, file, indent=4)

print("Dữ liệu giả đã được tạo thành công!")