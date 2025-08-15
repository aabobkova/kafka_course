import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# функция для десериализации JSON в объект Python
def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

# создание консьюмера
# group_id - уникальный ID, отличный от первого консьюмера
# enable_auto_commit=False - отключаем авто-коммит, будем управлять им вручную
# fetch_min_bytes=100 - минимальный объем данных (в байтах) для получения за один запрос
# fetch_max_wait_ms=5000 - максимальное время ожидания данных от брокера (в мс)
consumer_batch = KafkaConsumer(
    'messages-topic',
    group_id='batch-message-group',
    bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    fetch_min_bytes=100,
    fetch_max_wait_ms=5000,
    value_deserializer=json_deserializer
)

print("Запуск BatchMessageConsumer...")
try:
    while True:
        try:
            # метод poll() извлекает сразу несколько сообщений
            # timeout_ms - как долго ждать сообщений, если их нет
            # max_records - максимальное количество записей, которое вернет poll
            msg_pack = consumer_batch.poll(timeout_ms=5000, max_records=10)

            if not msg_pack:
                print("BatchConsumer | Нет новых сообщений, ожидание...")
                continue

            for tp, messages in msg_pack.items():
                print(f"\nBatchConsumer | Получена пачка из {len(messages)} сообщений из топика {tp.topic}, партиция {tp.partition}")
                for message in messages:
                    try:
                        # выводим полученное сообщение
                        print(f"  -> Обработка сообщения: {message.value} с оффсетом {message.offset}")
                    except json.JSONDecodeError:
                        print(f"BatchConsumer | Ошибка десериализации в пачке: {message.value}")
                    except Exception as e:
                        print(f"BatchConsumer | Ошибка при обработке сообщения в пачке: {e}")
            
            # коммитим оффсеты после обработки всей пачки
            consumer_batch.commit()
            print("BatchConsumer | Оффсеты для пачки закоммичены.\n")

        except KafkaError as e:
            print(f"BatchConsumer | Произошла ошибка Kafka в цикле: {e}")
            # Продолжаем работать

except KeyboardInterrupt:
    print("BatchConsumer | Остановка...")
finally:
    consumer_batch.close()
    print("BatchConsumer | Консьюмер остановлен.")
