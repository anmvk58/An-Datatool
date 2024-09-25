import sys
from tkinter import *
from tkinter import scrolledtext
# from wikipedia import *

def search_wiki():
    txt = text.get()           # Get what the user eneterd into the box
    # txt = wikipedia.page(txt)  # Search Wikipedia for results
    txt = txt.summary          # Store the returned information
    global texw
    textw = scrolledtext.ScrolledText(main,width=70,height=30)
    textw.grid(column=1, row=2,sticky=N+S+E+W)
    textw.config(background="light grey", foreground="black",
                 font='times 12 bold', wrap='word')
    textw.insert(END, txt)

main = Tk()
main.title("Search Wikipedia")
main.geometry('750x750')
main.configure(background='ivory3')

text = StringVar()

lblSearch = Label(main, text = 'Search String:').grid(row = 0, column = 0)
entSearch = Entry(main, textvariable = text, width = 50).grid(row = 0,
                                                          column = 1)

btn = Button(main, text = 'Search', bg='ivory2', width = 10,
             command = search_wiki).grid(row = 0, column = 10)

textw = scrolledtext.ScrolledText(main,width=70,height=20)
textw.grid(column=1, row=2,sticky=N+S+E+W)
textw.config(background="light grey", foreground="black",
             font='times 12 bold', wrap='word')
textw.insert(END, """
    Trong một bài nói chuyện trên TED năm 2009 có tiêu đề "Làm thế nào để sống đến hơn 100 tuổi", nhà báo Dan Buettner đã khám phá những đặc điểm lối sống của 5 nơi trên thế giới - nơi mọi người sống lâu nhất. Trong tất cả các "vùng xanh", như Buettner định nghĩa, người dân Okinawa có tuổi thọ cao nhất.

"Ở Mỹ, chúng ta chia cuộc sống trưởng thành của mình thành hai loại: Cuộc sống công việc và cuộc sống hưu trí. Ở Okinawa, thậm chí còn không có từ nào cho việc nghỉ hưu. Thay vào đó, chỉ chỉ có từ 'ikigai', về cơ bản có nghĩa là 'lý do để bạn thức dậy vào buổi sáng.'", anh ấy nói.

Buettner trích dẫn ikigai của một số người dân Okinawa: Đối với một ngư dân 101 tuổi, lý do đó là đánh bắt cá cho gia đình ông ba lần một tuần; đối với một cụ bà 102 tuổi, đó là ôm đứa cháu gái nhỏ xíu của bà (điều mà bà miêu tả là "giống như nhảy lên thiên đường"); đối với một bậc thầy karate 102 tuổi, đó là dạy võ thuật.

Trong nhiều năm, các nhà nghiên cứu đã cố gắng tìm ra những lý do đằng sau một cuộc sống lâu dài và khỏe mạnh. Mặc dù câu trả lời là sự kết hợp giữa gen tốt, chế độ ăn uống và tập thể dục, nhưng có những nghiên cứu cũng gợi ý rằng việc tìm kiếm ý nghĩa cuộc sống cũng là một nhân tố quan trọng.

Trong một nghiên cứu năm 2008 của Đại học Tohoku, các nhà nghiên cứu đã phân tích dữ liệu từ hơn 50.000 người tham gia (tuổi từ 40 đến 79) và phát hiện ra rằng những người có ikigai trong đời có nguy cơ mắc bệnh tim mạch và tỷ lệ tử vong thấp hơn. Nói cách khác, 95% số người được hỏi có ikigai vẫn còn sống sau 7 năm kể từ cuộc khảo sát ban đầu so với 83% người không có.

Không thể nói liệu ikigai có đảm bảo tuổi thọ trong cuộc sống thông qua nghiên cứu đơn lẻ này hay không, nhưng những phát hiện cho thấy rằng ý thức về mục đích sống có thể khuyến khích một người xây dựng cuộc sống hạnh phúc và phong phú.
""")

main.mainloop()

if __name__ == '__main__':
    for i in range(10):
        print(i)



























