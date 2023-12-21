import psycopg2

# PostgreSQL bağlantısı
conn = psycopg2.connect(
    dbname='chat_history',     # veritabanı adı
    user='myuser',             # kullanıcı adı
    password='111222',         # şifre
    host='localhost',          # Docker'da çalışıyor
    port='5432'                # port numarası
)

# cursor oluşturuyoruz
cursor = conn.cursor()

# sorgu yapıyoruz
cursor.execute('SELECT * FROM messages;')
rows = cursor.fetchall()

# ekrana yazdırma
for row in rows:
    print(row)

# cursor ve bağlantıyı kapatma
cursor.close()
conn.close()