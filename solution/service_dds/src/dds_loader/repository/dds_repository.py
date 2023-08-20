import uuid
from datetime import datetime
from typing import Any, Dict, List
import uuid
from uuid import UUID

from lib.pg import PgConnect
from pydantic import BaseModel
from datetime import datetime


class hUser(BaseModel):
    h_user_pk: UUID
    user_id: str
    load_dt: datetime
    load_src: str


class hRestaurant(BaseModel):
    h_restaurant_pk: UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str


class hOrder(BaseModel):
    h_order_pk: UUID
    order_id: int
    order_dt: datetime
    load_dt: datetime
    load_src: str


class hProduct(BaseModel):
    h_product_pk: UUID
    product_id: str
    load_dt: datetime
    load_src: str


class hCategory(BaseModel):
    h_category_pk: UUID
    category_name: str
    load_dt: datetime
    load_src: str


class lProductCategory(BaseModel):
    hk_product_category_pk: UUID
    h_product_pk: UUID
    h_category_pk: UUID
    load_dt: datetime
    load_src: str


class lOrderProduct(BaseModel):
    hk_order_product_pk: UUID
    h_order_pk: UUID
    h_product_pk: UUID
    load_dt: datetime
    load_src: str


class lOrderUser(BaseModel):
    hk_order_user_pk: UUID
    h_order_pk: UUID
    h_user_pk: UUID
    load_dt: datetime
    load_src: str


class lProductRest(BaseModel):
    hk_product_restaurant_pk: UUID
    h_product_pk: UUID
    h_restaurant_pk: UUID
    load_dt: datetime
    load_src: str


class sOrderCost(BaseModel):
    hk_order_cost_hashdiff: UUID
    h_order_pk: UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str


class sOrderStatus(BaseModel):
    hk_order_status_hashdiff: UUID
    h_order_pk: UUID
    status: str
    load_dt: datetime
    load_src: str


class sProductNames(BaseModel):
    hk_product_names_hashdiff: UUID
    h_product_pk: UUID
    name: str
    load_dt: datetime
    load_src: str


class sRestaurantNames(BaseModel):
    hk_restaurant_names_hashdiff: UUID
    h_restaurant_pk: UUID
    name: str
    load_dt: datetime
    load_src: str


class sUserNames(BaseModel):
    hk_user_names_hashdiff: UUID
    h_user_pk: UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str


class UserPrdCounter(BaseModel):
    user_id: str
    product_id: str
    product_name: str
    order_cnt: int


class UserCatCounter(BaseModel):
    user_id: str
    category_id: str
    category_name: str
    order_cnt: int


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_update(self, data: List[hUser]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.h_user VALUES (%s,%s,%s,%s)
                        ON CONFLICT (h_user_pk) do nothing
                    """,
                    [
                        (str(u.h_user_pk), u.user_id, u.load_dt, u.load_src)
                        for u in data
                    ],
                )

    def h_restarant_update(self, data: List[hRestaurant]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.h_restaurant VALUES (%s,%s,%s,%s)
                        ON CONFLICT (h_restaurant_pk) do nothing
                    """,
                    [
                        (str(u.h_restaurant_pk), u.restaurant_id, u.load_dt, u.load_src)
                        for u in data
                    ],
                )

    def h_order_update(self, data: List[hOrder]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.h_order VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (h_order_pk) do nothing
                    """,
                    [
                        (
                            str(u.h_order_pk),
                            u.order_id,
                            u.order_dt,
                            u.load_dt,
                            u.load_src,
                        )
                        for u in data
                    ],
                )

    def h_product_update(self, data: List[hProduct]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.h_product VALUES (%s,%s,%s,%s)
                        ON CONFLICT (h_product_pk) do nothing
                    """,
                    [
                        (str(u.h_product_pk), u.product_id, u.load_dt, u.load_src)
                        for u in data
                    ],
                )

    def h_category_update(self, data: List[hCategory]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.h_category VALUES (%s,%s,%s,%s)
                        ON CONFLICT (h_category_pk) do nothing
                    """,
                    [
                        (str(u.h_category_pk), u.category_name, u.load_dt, u.load_src)
                        for u in data
                    ],
                )

    def l_product_category_update(self, data: List[lProductCategory]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.l_product_category VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (hk_product_category_pk) do nothing
                    """,
                    [
                        (
                            str(u.hk_product_category_pk),
                            str(u.h_product_pk),
                            str(u.h_category_pk),
                            u.load_dt,
                            u.load_src,
                        )
                        for u in data
                    ],
                )

    def l_product_restaurant_update(self, data: List[lProductRest]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.l_product_restaurant VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (hk_product_restaurant_pk) do nothing
                    """,
                    [
                        (
                            str(u.hk_product_restaurant_pk),
                            str(u.h_product_pk),
                            str(u.h_restaurant_pk),
                            u.load_dt,
                            u.load_src,
                        )
                        for u in data
                    ],
                )

    def l_order_user_update(self, data: List[lOrderUser]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.l_order_user VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (hk_order_user_pk) do nothing
                    """,
                    [
                        (
                            str(u.hk_order_user_pk),
                            str(u.h_order_pk),
                            str(u.h_user_pk),
                            u.load_dt,
                            u.load_src,
                        )
                        for u in data
                    ],
                )

    def l_order_product_update(self, data: List[lOrderProduct]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.l_order_product VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (hk_order_product_pk) do nothing
                    """,
                    [
                        (
                            str(u.hk_order_product_pk),
                            str(u.h_order_pk),
                            str(u.h_product_pk),
                            u.load_dt,
                            u.load_src,
                        )
                        for u in data
                    ],
                )

    def s_order_cost_update(self, data: List[sOrderCost]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.s_order_cost VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (hk_order_cost_hashdiff) do nothing
                    """,
                    [
                        (
                            str(u.hk_order_cost_hashdiff),
                            str(u.h_order_pk),
                            u.cost,
                            u.payment,
                            u.load_dt,
                            u.load_src,
                        )
                        for u in data
                    ],
                )

    def s_order_status_update(self, data: List[sOrderStatus]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.s_order_status VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (hk_order_status_hashdiff) do nothing
                    """,
                    [
                        (
                            str(u.hk_order_status_hashdiff),
                            str(u.h_order_pk),
                            u.status,
                            u.load_dt,
                            u.load_src,
                        )
                        for u in data
                    ],
                )

    def s_product_names_update(self, data: List[sProductNames]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.s_product_names VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (hk_product_names_hashdiff) do nothing
                    """,
                    [
                        (
                            str(u.hk_product_names_hashdiff),
                            str(u.h_product_pk),
                            u.name,
                            u.load_dt,
                            u.load_src,
                        )
                        for u in data
                    ],
                )

    def s_restaurant_names_update(self, data: List[sRestaurantNames]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.s_restaurant_names VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT (hk_restaurant_names_hashdiff) do nothing
                    """,
                    [
                        (
                            str(u.hk_restaurant_names_hashdiff),
                            str(u.h_restaurant_pk),
                            u.name,
                            u.load_dt,
                            u.load_src,
                        )
                        for u in data
                    ],
                )

    def s_user_names_update(self, data: List[sUserNames]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO dds.s_user_names VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (hk_user_names_hashdiff) do nothing
                    """,
                    [
                        (
                            str(u.hk_user_names_hashdiff),
                            str(u.h_user_pk),
                            u.username,
                            u.userlogin,
                            u.load_dt,
                            u.load_src,
                        )
                        for u in data
                    ],
                )

    def get_user_prd_conter(self, user_pks: List[str]) -> List[UserPrdCounter]:
        where = ",".join([f"'{k}'" for k in user_pks])
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                       select distinct hu.h_user_pk  as user_id,hp.h_product_pk as product_id,spn."name" as product_name,count(distinct lou.h_order_pk)  as order_cnt from dds.h_user hu 
                        join dds.l_order_user lou on hu.h_user_pk  = lou.h_user_pk 
                        join dds.l_order_product lop on lop.h_order_pk = lou.h_order_pk
                        join dds.h_product hp on lop.h_product_pk = hp.h_product_pk 
                        join dds.s_product_names spn on spn.h_product_pk  = hp.h_product_pk 
                        where hu.h_user_pk in ({where})
                        group by hu.h_user_pk,hp.h_product_pk,spn."name" 
                    """
                )
                fetched = cur.fetchall()
                result = [
                    UserPrdCounter(
                        user_id=str(r[0]),
                        product_id=str(r[1]),
                        product_name=r[2],
                        order_cnt=r[3],
                    )
                    for r in fetched
                ]

            return result

    def get_user_cat_conter(self, user_pks: List[str]) -> List[UserCatCounter]:
        where = ",".join([f"'{k}'" for k in user_pks])
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    select  hu.h_user_pk  as user_id,hc.h_category_pk as category_id,hc.category_name ,count(distinct lou.h_order_pk)  as order_cnt from dds.h_user hu 
                            join dds.l_order_user lou on hu.h_user_pk  = lou.h_user_pk 
                            join dds.l_order_product lop on lop.h_order_pk = lou.h_order_pk
                            join dds.h_product hp on lop.h_product_pk = hp.h_product_pk 		
                            join dds.l_product_category lpc on hp.h_product_pk  = lpc.h_product_pk 
                            join dds.h_category hc on hc.h_category_pk  = lpc.h_category_pk 
                            where hu.h_user_pk in ({where})
                            group by hu.h_user_pk ,hc.h_category_pk ,hc.category_name	
                    """
                )
                fetched = cur.fetchall()
                result = [
                    UserCatCounter(
                        user_id=str(r[0]),
                        category_id=str(r[1]),
                        category_name=r[2],
                        order_cnt=r[3],
                    )
                    for r in fetched
                ]

            return result
