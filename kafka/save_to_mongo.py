from pymongo import MongoClient

# Kết nối đến MongoDB
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["movie_database"]
collection = db["cleaned_movies"]

def save_to_mongo(data):
    """Lưu dữ liệu đã làm sạch vào MongoDB"""
    if data:  # Chỉ lưu nếu dữ liệu hợp lệ
        collection.insert_one(data)
        print(f"✅ Đã lưu vào MongoDB: {data}")
