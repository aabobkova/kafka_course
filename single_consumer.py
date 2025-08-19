import json
import logging
import os
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('SingleMessageConsumerApp')

# функция для десериализации JSON в объект Python
def json_deserializer(data):
    try:
        return json.loads(data.decode('utf-8'))
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка десериализации JSON: {e} для данных: {data}")
        return None

bootstrap_servers_str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
bootstrap_servers = bootstrap_servers_str.split(',')

# создание консьюмера
# group_id - уникальный идентификатор группы консьюмеров
# bootstrap_servers - список брокеров для подключения
# auto_offset_reset='earliest' - начинать чтение с самого раннего сообщения, если оффсет не найден
# enable_auto_commit=True - автоматический коммит оффсетов
# auto_commit_interval_ms=1000 - интервал автоматического коммита в миллисекунда.
# value_deserializer - функция для десериализации сообщений
consumer_single = KafkaConsumer(
    'test_topic_v2',
    group_id='single-message-group',
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000,
    value_deserializer=json_deserializer
)

logger.info("Запуск SingleMessageConsumer...")
try:
    for message in consumer_single:
        if message.value is None:
            logger.warning(f"SingleConsumer | Пропущено сообщение из-за ошибки десериализации: {message.value} "
                           f"| Топик: {message.topic} | Партиция: {message.partition} | Оффсет: {message.offset}")
            continue

        try:
            message_data = message.value
            content = message_data.get('content', 'Нет содержимого')
            logger.info(f"SingleConsumer | Получено сообщение: {message_data} "
                        f"| Топик: {message.topic} | Партиция: {message.partition} | Оффсет: {message.offset}")
            logger.info(f"SingleConsumer | Обработка: '{content}'")
            # time.sleep(0.1)
            
        except Exception as e:
            logger.error(f"SingleConsumer | Ошибка при обработке сообщения: {e} для сообщения: {message.value}")

except KafkaError as e:
    logger.error(f"SingleConsumer | Произошла ошибка Kafka: {e}")
except KeyboardInterrupt:
    logger.info("SingleConsumer | Остановка...")
finally:
    consumer_single.close()
    logger.info("SingleConsumer | Консьюмер остановлен.")
