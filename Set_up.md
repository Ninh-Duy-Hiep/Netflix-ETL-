**1. Khởi động zookeeper và server**
`bin\windows\zookeeper-server-start.bat config\zookeeper.properties`
`bin\windows\kafka-server-start.bat config\server.properties`

**2. Khởi tạo data và chạy producer**
**3. Khởi động Mongodb**
`net start MongoDB`
**4.Chạy consumer**
# Mở MongoDB Shell và kiểm tra db 
Bật cmd 
`mongosh`
`use movie_database`
Lệnh này để in ra db `db.cleaned_movies.find().pretty()`