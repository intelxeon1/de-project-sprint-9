import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository import CdmRepository


app = Flask(__name__)

config = AppConfig()


@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    config = AppConfig()

    proc = CdmMessageProcessor(
        consumer=config.kafka_consumer(),        
        cdm_repository=CdmRepository(config.pg_warehouse_db()),
        batch_size=100,
        logger=app.logger,
    )
    flag = 1
    while flag:
        proc.run()
