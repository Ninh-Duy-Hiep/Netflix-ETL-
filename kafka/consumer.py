from confluent_kafka import Consumer, KafkaError
import json
import re
from langdetect import detect, LangDetectException
import langid
from symspellpy import SymSpell, Verbosity
from save_to_mongo import save_to_mongo

sym_spell = SymSpell(max_dictionary_edit_distance=2, prefix_length=7)
sym_spell.load_dictionary("./data/frequency_dictionary_en_82_765.txt", term_index=0, count_index=1)
sym_spell.load_bigram_dictionary("./data/frequency_bigramdictionary_en_243_342.txt", term_index=0, count_index=2)
# 🔹 Cấu hình Kafka Consumer
conf = {
    'bootstrap.servers': '10.6.98.110:9092',  # Đảm bảo địa chỉ IP và port đúng
    'group.id': 'movie-data-consumer',
    'auto.offset.reset': 'earliest',
    # 'auto.offset.reset': 'latest',
}
consumer = Consumer(conf)
consumer.subscribe(['movie_data'])  

# 🔹 Hàm làm sạch dữ liệu
def clean_data(data):
    """Làm sạch dữ liệu từ khóa tìm kiếm"""

    # 1️⃣ Chuẩn hóa từ khóa
    search_keyword = data.get("keyword", "").strip().lower()
    search_keyword = re.sub(r"[^\w\s]", "", search_keyword)  # Bỏ ký tự đặc biệt

    # 2️⃣ Kiểm tra lỗi chính tả trước
    suggestions = sym_spell.lookup(search_keyword, Verbosity.CLOSEST, max_edit_distance=2)
    if suggestions:
        search_keyword = suggestions[0].term  # Lấy từ đúng nhất

    # 3️⃣ Xác định ngôn ngữ
    language, _ = langid.classify(search_keyword)

    # 4️⃣ Loại bỏ từ khóa không hợp lệ
    if len(search_keyword) < 2 or search_keyword.isdigit():
        return None  

    # ✅ Cập nhật lại dữ liệu
    data["keyword"] = search_keyword
    data["language"] = language

    return data



# 🔹 Nhận và xử lý dữ liệu từ Kafka
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Lỗi Kafka: {msg.error()}")
        else:
            record = json.loads(msg.value().decode('utf-8'))
            print(f"\n➡️ Dữ liệu gốc nhận được: {record}")

            cleaned_record = clean_data(record)
            
            if cleaned_record:  
                save_to_mongo(cleaned_record)  

except KeyboardInterrupt:
    print("\nDừng Consumer...")
finally:
    consumer.close()
