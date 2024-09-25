import art
import tkinter as tk
from src.GUI.Page_Page import Page

class Guide(Page):
   def __init__(self, *args, **kwargs):
       Page.__init__(self, *args, **kwargs)
       hdsd = """Mục đích sử dụng:
- Tool dùng để so sánh dữ liệu của 2 truy vấn bất kỳ, thuộc 1 hoặc 2 database khác nhau. 
- Các loại Database hỗ trợ: Oracle, SQL Server, DB2 (Sẽ update thêm trong tương lai)
- Nếu dữ liệu không khớp, kết quả chi tiết sẽ được trả ra trong file chỉ tiết, hãy xem ở mục Log run để truy cập chính xác !

Hướng dẫn sử dụng: 
- Truy vấn tạm thời không nên cho dấu ; vào cuối. 
- Không phân biệt chữ hoa, chữ thường, thậm chí là cả thứ tự các cột, nhưng bắt buộc truy vấn phải có số lượng cột giống nhau. 
- Không ngán lệnh phức tạp (WITH AS ... )  ^_^, miễn là lệnh đúng ! 
- Mã Lỗi đang được trả ra nguyên bản, vì vậy đôi khi sẽ khó hiểu, hãy gửi lại mã lỗi cho Dev"""
       label = tk.Label(self, text="HƯỚNG DẪN SỬ DỤNG", font=("Arial", 20, "bold"), fg='#C70039')
       label.pack(pady=(20,10), side=tk.TOP, fill=tk.X, expand=False)

       # flat, groove, raised, ridge, solid, or sunken
       T = tk.Text(self, font=("Courier New", 12), padx=10, pady=5, fg='#444444')
       T.config(wrap='word')
       T.insert(tk.END, hdsd)
       T.tag_configure("center", justify='center')
       T.insert(tk.END, '\n' + art.text2art('A-DATA TOOL'), 'center')
       T.configure(state="disabled")
       T.pack(padx=5, pady=5, side=tk.TOP, fill=tk.BOTH, expand=True)
