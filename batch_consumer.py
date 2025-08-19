import json
import logging
import os
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('BatchMessageConsumerApp')

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
# group_id - уникальный ID, отличный от первого консьюмера
# enable_auto_commit=False - отключаем авто-коммит, будем управлять им вручную
# fetch_min_bytes=100 - минимальный объем данных (в байтах) для получения за один запрос
# fetch_max_wait_ms=5000 - максимальное время ожидания данных от брокера (в мс)
consumer_batch = KafkaConsumer(
    'test_topic_v2',
    group_id='batch-message-group',
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    fetch_min_bytes=100,
    fetch_max_wait_ms=5000,
    value_deserializer=json_deserializer
)

logger.info("Запуск BatchMessageConsumer...")
try:
    while True:
        try:
            msg_pack = consumer_batch.poll(timeout_ms=5000, max_records=10)

            if not msg_pack:
                logger.info("BatchConsumer | Нет новых сообщений, ожидание...")
                continue

            for tp, messages in msg_pack.items():
                logger.info(f"\nBatchConsumer | Получена пачка из {len(messages)} сообщений из топика {tp.topic}, партиция {tp.partition}")
                for message in messages:
                    if message.value is None:
                        logger.warning(f"BatchConsumer | Пропущено сообщение из-за ошибки десериализации в пачке: {message.value} "
                                       f"| Топик: {message.topic} | Партиция: {message.partition} | Оффсет: {message.offset}")
                        continue
                    try:
                        message_data = message.value
                        content = message_data.get('content')
                        logger.info(f"  -> Обработка сообщения: {message_data} с оффсетом {message.offset}")
                        # time.sleep(0.1)
                    except Exception as e:
                        logger.error(f"BatchConsumer | Ошибка при обработке сообщения в пачке: {e} для сообщения: {message.value}")
            
            consumer_batch.commit()
            logger.info("BatchConsumer | Оффсеты для пачки закоммичены.\n")

        except KafkaError as e:
            logger.error(f"BatchConsumer | Произошла ошибка Kafka в цикле: {e}")

except KeyboardInterrupt:
    logger.info("BatchConsumer | Остановка...")
finally:
    consumer_batch.close()
    logger.info("BatchConsumer | Консьюмер остановлен.")
