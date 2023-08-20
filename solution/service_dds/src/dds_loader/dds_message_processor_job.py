from datetime import datetime
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect
from dds_loader.repository import (
    DdsRepository,
    hUser,
    hRestaurant,
    hOrder,
    hProduct,
    hCategory,
    lProductCategory,
    lProductRest,
    lOrderUser,
    lOrderProduct,
    sUserNames,
    sRestaurantNames,
    sOrderCost,
    sOrderStatus,
    sProductNames,
)
from typing import List, Set

import uuid


class MyData:
    def __init__(self):
        self._list = []
        self._pks = set()

    def append(self, element, pk):
        if pk not in self._pks:
            self._list.append(element)
            self._pks.add(pk)


class DdsMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        dds_repository: DdsRepository,
        batch_size: int,
        logger: Logger,
    ) -> None:

        self._consumer = consumer
        self._producer = producer

        self._dds_repository = dds_repository
        self._batch_size = batch_size
        self._logger = logger
        self._batch_size = batch_size

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

        user_data = MyData()
        rest_data = MyData()
        order_data = MyData()
        category_data = MyData()
        product_data = MyData()

        prod_cat_data = MyData()
        order_prod_data = MyData()
        order_user_data = MyData()
        rest_prod_data = MyData()

        order_cost = MyData()
        user_names = MyData()
        prod_names = MyData()
        rest_names = MyData()
        order_status = MyData()

        for message in msgs:
            payload = message["payload"]
            user = payload["user"]
            rest = payload["restaurant"]

            user_pk = uuid.uuid3(uuid.NAMESPACE_DNS, user["id"])

            self._logger.debug(f'user_id: {user["id"]}')
            self._logger.debug(f"user_pk: {str(user_pk)}")
            user_data.append(
                hUser(
                    h_user_pk=user_pk,
                    user_id=user["id"],
                    load_dt=datetime.now(),
                    load_src=self._consumer.topic,
                ),
                pk=user_pk,
            )
            user_name_pk = uuid.uuid3(uuid.NAMESPACE_DNS, user["id"]+user['name']+user.get('login',''))
            user_names.append(
                sUserNames(
                    hk_user_names_hashdiff=user_name_pk,
                    h_user_pk=user_pk,
                    username=user['name'],
                    userlogin=user.get('login',''),
                    load_dt=datetime.now(),
                    load_src=self._consumer.topic,
                ),
                user_name_pk
            )

            rest_pk = uuid.uuid3(uuid.NAMESPACE_DNS, rest["id"])
            self._logger.debug(f'rest id: {rest["id"]}')
            self._logger.debug(f"rest pk: {str(rest_pk)}")
            rest_data.append(
                hRestaurant(
                    h_restaurant_pk=rest_pk,
                    restaurant_id=rest["id"],
                    load_dt=datetime.now(),
                    load_src=self._consumer.topic,
                ),
                pk=rest_pk,
            )
            
            rest_name_pk = uuid.uuid3(uuid.NAMESPACE_DNS, rest["id"] + rest['name'])
            rest_names.append(
                sRestaurantNames(
                    hk_restaurant_names_hashdiff=rest_name_pk,
                    h_restaurant_pk=rest_pk,
                    name = rest['name'],
                    load_dt=datetime.now(),
                    load_src=self._consumer.topic,                    
                ),
                pk=rest_name_pk
            )
            
            
            order_pk = uuid.uuid3(uuid.NAMESPACE_DNS, str(payload["id"]))
            order_data.append(
                hOrder(
                    h_order_pk=order_pk,
                    order_id=payload["id"],
                    order_dt=payload["date"],
                    load_dt=datetime.now(),
                    load_src=self._consumer.topic,
                ),
                order_pk,
            )
            order_cost_pk = uuid.uuid3(
                uuid.NAMESPACE_DNS,
                str(payload["id"]) + str(payload["cost"]) + str(payload["payment"]),
            )
            order_cost.append(
                sOrderCost(
                    hk_order_cost_hashdiff=order_cost_pk,
                    h_order_pk=order_pk,
                    cost=payload["cost"],
                    payment=payload["payment"],
                    load_dt=datetime.now(),
                    load_src=self._consumer.topic,
                ),
                order_cost_pk,
            )
            order_status_pk = uuid.uuid3(
                uuid.NAMESPACE_DNS, str(payload["id"]) + payload["status"]
            )
            order_status.append(
                sOrderStatus(
                    hk_order_status_hashdiff=order_status_pk,
                    h_order_pk=order_pk,
                    status=payload["status"],
                    load_dt=datetime.now(),
                    load_src=self._consumer.topic,
                ),
                order_status_pk,
            )

            l_order_user_pk = uuid.uuid3(
                uuid.NAMESPACE_DNS, str(payload["id"]) + user["id"]
            )
            order_user_data.append(
                lOrderUser(
                    hk_order_user_pk=l_order_user_pk,
                    h_order_pk=order_pk,
                    h_user_pk=user_pk,
                    load_dt=datetime.now(),
                    load_src=self._consumer.topic,
                ),
                l_order_user_pk,
            )

            for p in payload["products"]:
                p_pk = uuid.uuid3(uuid.NAMESPACE_DNS, p["id"])
                product_data.append(
                    hProduct(
                        h_product_pk=p_pk,
                        product_id=p["id"],
                        load_dt=datetime.now(),
                        load_src=self._consumer.topic,
                    ),
                    p_pk,
                )
                p_name_pk = uuid.uuid3(uuid.NAMESPACE_DNS, p["id"]+p['name'])
                prod_names.append(
                    sProductNames(
                        hk_product_names_hashdiff=p_name_pk,
                        h_product_pk=p_pk,
                        name = p['name'],
                        load_dt=datetime.now(),
                        load_src=self._consumer.topic,
                    ),
                    p_name_pk
                )

                l_order_prod_pk = uuid.uuid3(
                    uuid.NAMESPACE_DNS, str(payload["id"]) + p["id"]
                )
                order_prod_data.append(
                    lOrderProduct(
                        hk_order_product_pk=l_order_prod_pk,
                        h_order_pk=order_pk,
                        h_product_pk=p_pk,
                        load_dt=datetime.now(),
                        load_src=self._consumer.topic,
                    ),
                    l_order_prod_pk,
                )

                l_rest_prod_pk = uuid.uuid3(uuid.NAMESPACE_DNS, rest["id"] + p["id"])
                rest_prod_data.append(
                    lProductRest(
                        hk_product_restaurant_pk=l_rest_prod_pk,
                        h_product_pk=p_pk,
                        h_restaurant_pk=rest_pk,
                        load_dt=datetime.now(),
                        load_src=self._consumer.topic,
                    ),
                    l_rest_prod_pk,
                )

                c_pk = uuid.uuid3(uuid.NAMESPACE_DNS, p["category"])
                category_data.append(
                    hCategory(
                        h_category_pk=c_pk,
                        category_name=p["category"],
                        load_dt=datetime.now(),
                        load_src=self._consumer.topic,
                    ),
                    c_pk,
                )
                l_p_c_pk = uuid.uuid3(uuid.NAMESPACE_DNS, p["id"] + p["category"])
                prod_cat_data.append(
                    lProductCategory(
                        hk_product_category_pk=l_p_c_pk,
                        h_product_pk=p_pk,
                        h_category_pk=c_pk,
                        load_dt=datetime.now(),
                        load_src=self._consumer.topic,
                    ),
                    pk=l_p_c_pk,
                )

        self._dds_repository.h_user_update(user_data._list)
        self._dds_repository.h_restarant_update(rest_data._list)
        self._dds_repository.h_order_update(order_data._list)
        self._dds_repository.h_product_update(product_data._list)
        self._dds_repository.h_category_update(category_data._list)
        self._dds_repository.l_product_category_update(prod_cat_data._list)
        self._dds_repository.l_order_user_update(order_user_data._list)
        self._dds_repository.l_order_product_update(order_prod_data._list)
        self._dds_repository.l_product_restaurant_update(rest_prod_data._list)
        self._dds_repository.s_order_cost_update(order_cost._list)
        self._dds_repository.s_order_status_update(order_status._list)
        self._dds_repository.s_product_names_update(prod_names._list)
        self._dds_repository.s_restaurant_names_update(rest_names._list)
        self._dds_repository.s_user_names_update(user_names._list)
        
        user_pks = [ str(u.h_user_pk) for u in user_data._list]
        data = self._dds_repository.get_user_prd_conter(user_pks)        
        for d in data:
            self._producer.produce({"user_product_counters" : d.model_dump()})
            
        data = self._dds_repository.get_user_cat_conter(user_pks)
        for d in data:
            self._producer.produce({"user_category_counters" : d.model_dump()})
            

        self._logger.info(f"{datetime.utcnow()}: FINISH")
