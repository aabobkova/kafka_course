import json
import time
import uuid
import logging
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('KafkaProducerApp')

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

bootstrap_servers_str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
bootstrap_servers = bootstrap_servers_str.split(',')

# создание продюсера
# bootstrap_servers - список хостов и портов брокеров Kafka
# value_serializer - функция для сериализации сообщений
# acks='all' - гарантия доставки at least once. Продюсер будет ждать подтверждения от всех реплик.
# retries=5 - количество повторных попыток отправки в случае ошибки.
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=json_serializer,
    acks='all',
    retries=5 
)

topic_name = 'test_topic_v2'

logger.info("Запуск продюсера...")
try:
    for i in range(20):
        message_id = str(uuid.uuid4())
        message_content = f"Это сообщение номер {i+1}"
        message = Message(message_id=message_id, content=message_content)

        logger.info(f"Отправка сообщения: {message.to_dict()}")

        future = producer.send(topic_name, value=message)
        
        record_metadata = future.get(timeout=10)
        logger.info(f"Сообщение отправлено в топик '{record_metadata.topic}' "
                    f"партицию {record_metadata.partition} "
                    f"с оффсетом {record_metadata.offset}")

        time.sleep(1) # Пауза между отправками

except KafkaError as e:
    logger.error(f"Произошла ошибка Kafka: {e}")
except Exception as e:
    logger.error(f"Произошла непредвиденная ошибка: {e}")
finally:
    producer.flush()
    producer.close()
    logger.info("Продюсер остановлен.")
