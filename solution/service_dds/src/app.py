import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor
from dds_loader.repository.dds_repository import DdsRepository

app = Flask(__name__)

config = AppConfig()


@app.get("/health")
def hello_world():
    return "healthy"


if __name__ == "__main__":
    app.logger.setLevel(logging.DEBUG)

    config = AppConfig()

    # Инициализируем процессор сообщений.
    # Пока он пустой. Нужен для того, чтобы потом в нем писать логику обработки сообщений из Kafka.
    proc = DdsMessageProcessor(
        consumer=config.kafka_consumer(),
        producer=config.kafka_producer(),
        dds_repository=DdsRepository(config.pg_warehouse_db()),
        batch_size=config.DEFAULT_JOB_INTERVAL,
        logger=app.logger,
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL
    )
    scheduler.start()

    app.run(debug=True, host="0.0.0.0", use_reloader=False)
