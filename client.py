import tkinter as tk
from confluent_kafka import Producer, Consumer, KafkaException
import psycopg2
import json


# Kafka producer
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# PostgreSQL bağlantısı
conn = psycopg2.connect(
    dbname='chat_history',     # Veritabanı adı
    user='myuser',             # PostgreSQL kullanıcı adı
    password='111222',         # PostgreSQL şifre
    host='localhost',          # PostgreSQL sunucu adresi (Docker'da çalışıyorsa)
    port='5432'                # PostgreSQL port numarası
)

# Tkinter'dan input aldığımız mesajı Kafka'ya gönderiyoruz
def send_message():
    message = entry.get()
    sender = userName.get()
    producer.produce('chat_messages', value=json.dumps({'sender': sender, 'message': message}))
    entry.delete(0, tk.END)
    display_messages()

# PostgreSQL'den mesajları çekip Tkinter ile gösteriyoruz.  
def display_messages():
    cursor = conn.cursor()
    cursor.execute("SELECT sender, message_content FROM messages ORDER BY id DESC LIMIT 42")
    messages = cursor.fetchall()
    message_history.delete(1.0, tk.END)
    for message in reversed(messages):
        message_history.insert(tk.END, f"{message[0]}: {message[1]}\n")
    root.after(2000, display_messages)

# Tkinter ile arayüz oluşturuyoruz
root = tk.Tk()
root.title("Chat Uygulaması")

label = tk.Label(root, text="Username:")
label.pack()

userName = tk.Entry(root, width=50)
userName.pack()

label = tk.Label(root, text="Mesajınızı girin:")
label.pack()

entry = tk.Entry(root, width=50)
entry.pack()

send_button = tk.Button(root, text="Gönder", command=send_message)
send_button.pack()

message_history = tk.Text(root, height=100, width=80)
message_history.pack()

# ana fonksiyon
root.after(2000, display_messages)
root.mainloop()

# producer ve PostgreSQL bağlantısı kapatıyoruz
producer.flush()
conn.close()


