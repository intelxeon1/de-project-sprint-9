import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor
from dds_loader.repository.dds_repository import DdsRepository
from  logging import Logger
import time 
import sys

logger = logging.getLogger()


if __name__ == "__main__":
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)    

    config = AppConfig()

    # Инициализируем процессор сообщений.
    # Пока он пустой. Нужен для того, чтобы потом в нем писать логику обработки сообщений из Kafka.
    proc = DdsMessageProcessor(
        consumer=config.kafka_consumer(),
        producer=config.kafka_producer(),
        dds_repository=DdsRepository(config.pg_warehouse_db()),
        batch_size=config.DEFAULT_JOB_INTERVAL,
        logger=logger,
    )

    proc.run()
    
