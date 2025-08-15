import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# функция для десериализации JSON в объект Python
def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

# создание консьюмера
# group_id - уникальный идентификатор группы консьюмеров
# bootstrap_servers - список брокеров для подключения
# auto_offset_reset='earliest' - начинать чтение с самого раннего сообщения, если оффсет не найден
# enable_auto_commit=True - автоматический коммит оффсетов
# auto_commit_interval_ms=1000 - интервал автоматического коммита в миллисекунда.
# value_deserializer - функция для десериализации сообщений
consumer_single = KafkaConsumer(
    'test_topic',
    group_id='single-message-group',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    value_deserializer=json_deserializer
)

print("Запуск SingleMessageConsumer...")
try:
    for message in consumer_single:
        try:
            # Выводим в консоль
            print(f"SingleConsumer | Получено сообщение: {message.value} "
                  f"| Топик: {message.topic} "
                  f"| Партиция: {message.partition} "
                  f"| Оффсет: {message.offset}")

        except json.JSONDecodeError:
            print(f"SingleConsumer | Ошибка десериализации сообщения: {message.value}")
        except Exception as e:
            print(f"SingleConsumer | Ошибка при обработке сообщения: {e}")

except KafkaError as e:
    print(f"SingleConsumer | Произошла ошибка Kafka: {e}")
except KeyboardInterrupt:
    print("SingleConsumer | Остановка...")
finally:
    consumer_single.close()
    print("SingleConsumer | Консьюмер остановлен.")
