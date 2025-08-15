import json
import time
import uuid
from kafka import KafkaProducer
from kafka.errors import KafkaError

# класс сообщения
class Message:
    def __init__(self, message_id, content):
        self.message_id = message_id
        self.content = content

    def to_dict(self):
        return {
            "message_id": self.message_id,
            "content": self.content
        }

# функция для сериализации объекта в JSON
def json_serializer(data):
    return json.dumps(data.to_dict()).encode('utf-8')

# Создание продюсера
# bootstrap_servers - список хостов и портов брокеров Kafka
# value_serializer - функция для сериализации сообщений
# acks='all' - гарантия доставки at least once. Продюсер будет ждать подтверждения от всех реплик.
# retries=5 - количество повторных попыток отправки в случае ошибки.
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer,
    acks='all',
    retries=5
)

# название топика
topic_name = 'test_topic'

print("Запуск продюсера...")
try:
    for i in range(20):
        # создаем новое сообщение
        message_id = str(uuid.uuid4())
        message_content = f"Это сообщение номер {i+1}"
        message = Message(message_id=message_id, content=message_content)

        # выводим в консоль
        print(f"Отправка сообщения: {message.to_dict()}")

        # асинхронная отправка сообщения
        future = producer.send(topic_name, value=message)
        
        # блокируемся до успешной отправки или ошибки
        record_metadata = future.get(timeout=10)
        print(f"Сообщение отправлено в топик '{record_metadata.topic}' "
              f"партицию {record_metadata.partition} "
              f"с оффсетом {record_metadata.offset}")

        time.sleep(1) # пауза между отправками

except KafkaError as e:
    print(f"Произошла ошибка Kafka: {e}")
except Exception as e:
    print(f"Произошла непредвиденная ошибка: {e}")
finally:
    # закрываем продюсер
    producer.flush()
    producer.close()
    print("Продюсер остановлен.")
