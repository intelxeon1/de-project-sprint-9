import uuid
from datetime import datetime
from typing import Any, Dict, List
import uuid
from uuid import UUID

from lib.pg import PgConnect
from pydantic import BaseModel

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


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_category_counters_update(self, data: List[UserCatCounter]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO cdm.user_category_counters (user_id,category_id,category_name,order_cnt) VALUES (%s,%s,%s,%s)
                        ON CONFLICT (user_id,category_id) do update SET order_cnt = EXCLUDED.order_cnt
                    """,
                    [
                        (u.user_id,u.category_id,u.category_name,u.order_cnt)
                        for u in data
                    ],
                )
                
    def user_product_counters_update(self, data: List[UserPrdCounter]):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                        INSERT INTO cdm.user_product_counters (user_id,product_id,product_name,order_cnt) VALUES (%s,%s,%s,%s)
                        ON CONFLICT (user_id,product_id) do update SET order_cnt = EXCLUDED.order_cnt
                    """,
                    [
                        (u.user_id,u.product_id,u.product_name,u.order_cnt)
                        for u in data
                    ],
                )                