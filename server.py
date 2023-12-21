from confluent_kafka import Consumer, KafkaException
import psycopg2
import json

# Kafka consumer yapılandırması
conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'}

# PostgreSQL bağlantısı
conn = psycopg2.connect(
    dbname='chat_history',     # veritabanı adı
    user='myuser',             # kullanıcı adı
    password='111222',         # şifre
    host='localhost',          # Docker'da çalışıyor
    port='5432'                # port numarası
)

# consumer oluşturup topic'e bağlandık
consumer = Consumer(conf)
topic = 'chat_messages'
consumer.subscribe([topic])

# PostgreSQL bağlantısı üzerinden veritabanında tablo oluşturduk
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,
        sender VARCHAR(50),
        message_content TEXT
    );
''')
conn.commit()
cursor.close()

# mesajları PostgreSQL'e kaydetme fonksiyonu
def save_to_postgres(message):
    cursor = conn.cursor()
    try:
        data = json.loads(message.value())
        sender = data['sender']
        message_content = data['message']
        cursor.execute("INSERT INTO messages (sender, message_content) VALUES (%s, %s)", (sender, message_content))
        conn.commit()
    except (psycopg2.Error, ValueError) as e:
        print(f"Error while inserting into PostgreSQL: {e}")
        conn.rollback()
    finally:
        cursor.close()

# mesajları okuyup terminale yazdırıyor ve veritabanına kaydediyoruz
while True:
    try:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue
        save_to_postgres(msg)
        message = json.loads(msg.value())
        print('{}: {}'.format(message['sender'], message['message']))
    except KeyboardInterrupt:
        break

# consumer ve PostgreSQL bağlantısı kapatıyoruz
consumer.close()
conn.close()