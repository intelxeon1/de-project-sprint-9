from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository import CdmRepository, UserPrdCounter, UserCatCounter
from typing import List


class CdmMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        cdm_repository: CdmRepository,
        batch_size: int,
        logger: Logger,
    ) -> None:

        self._logger = logger
        self._consumer = consumer
        self._batch_size = batch_size
        self._cdm_repository = cdm_repository

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        msgs = []
        for i in range(self._batch_size):
            message = self._consumer.consume(10)
            if not message:
                break
            else:
                msgs.append(message)
        if len(msgs) == 0:
            return
        usr_prod_data: List[UserPrdCounter] = []
        usr_cat_data: List[UserCatCounter] = []
        for m in msgs:
            r = m.get("user_product_counters")
            if r:
                usr_prod_data.append(
                    UserPrdCounter(
                        user_id=r["user_id"],
                        product_id=r["product_id"],
                        product_name=r["product_name"],
                        order_cnt=r["order_cnt"]
                    )
                )
            r = m.get("user_category_counters")
            if r:
                usr_cat_data.append(UserCatCounter(
                    user_id=r['user_id'],
                    category_id=r['category_id'],
                    category_name=r['category_name'],
                    order_cnt=r["order_cnt"]                  
                ))

        self._cdm_repository.user_category_counters_update(usr_cat_data)
        self._cdm_repository.user_product_counters_update(usr_prod_data)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
