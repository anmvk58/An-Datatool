# An Data Tool

## 1.General

Đây là tools test dữ liệu được viết bằng ngôn ngữ Python, có 2 mode kết nối sử dụng ODBC và JDBC nhằm mục đích so sánh
dữ liệu từ 2 query thuộc 2 Database khác nhau. (Khuyên dùng ODBC để có hiệu năng tốt hơn)

*Updated on 9/9/2024: Hiện tại Tools hỗ trợ các loại Database sau:*

- Oracle
- Microsoft SQL Server
- IBM DB2
- MySQL
- AWS RedShift

## 2.Prerequisite

Chương trình có 2 mode kết nối là ODBC và JDBC. Sử dụng ODBC cho hiệu năng tốt hơn, tuy nhiên máy tính cần cài đặt ODBC
Driver cho tất cả các loại Database thì mới có thể sử dụng được. Do đó Anmv1 đã phát triển thêm để có thể sử dụng JDBC
thông qua các package đóng gói sẵn.

Các điều kiện tiên quyết cần chuẩn bị như sau:

- Cài đặt Python 3.8, 3.9 hoặc 3.10 (chưa test lại với 3.12)
- Tạo venv base on Python ở bước 1 (hoặc cài các gói vào thẳng môi trường Python gốc)
- Cài đặt các packages đầy đủ trong requirements.txt
- Cài đặt ODBC tương ứng với các loại Database
- Cài đặt JDK phiên bản >= 8 và set biến môi trường JAVA_HOME, set Path variables trong hệ thống windows.

## 3.Run

Để chạy chương trình, làm theo các bước sau:

1. Active Venv đã chuẩn bị ở bước 2
2. cd vào src/GUI
3. run file Compare_GUI_v2.py
    
---
*Chú ý: add vào gitignore src/connection_defi2.json để tránh push credentials lên resource nếu bạn muốn contribute thêm !*