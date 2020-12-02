import json
from datetime import datetime

from peewee import (
    AutoField,
    CharField,
    Database,
    DateTimeField,
    FloatField,
    Model,
    SqliteDatabase,
    BooleanField,
    chunked
)

database_path = "jq_database.db"
db = SqliteDatabase(database_path)


class JqBarData(Model):
    id = AutoField()

    index: str = CharField()
    datetime: datetime = DateTimeField()
    interval: str = CharField()

    open: float = FloatField(null=True)
    close: float = FloatField(null=True)
    low: float = FloatField(null=True)
    high: float = FloatField(null=True)
    volume: float = FloatField(null=True)
    money: float = FloatField(null=True)

    factor: float = FloatField(null=True)
    high_limit: float = FloatField(null=True)
    low_limit: float = FloatField(null=True)
    avg: float = FloatField(null=True)
    pre_close: float = FloatField(null=True)
    paused: bool = BooleanField(null=True)

    class Meta:
        database = db
        indexes = ((("index", "datetime", "interval"), True),)


class JqStockInfo(Model):
    id = AutoField()

    index: str = CharField()
    display_name: str = CharField(null=True)
    name: str = CharField(null=True)
    type: str = CharField(null=True)

    start_date: datetime = DateTimeField(null=True)
    end_date: datetime = DateTimeField(null=True)

    sw_l1_code: str = CharField(null=True)
    sw_l1_name: str = CharField(null=True)
    sw_l2_code: str = CharField(null=True)
    sw_l2_name: str = CharField(null=True)
    sw_l3_code: str = CharField(null=True)
    sw_l3_name: str = CharField(null=True)

    zjw_code: str = CharField(null=True)
    zjw_name: str = CharField(null=True)
    jq_l1_code: str = CharField(null=True)
    jq_l1_name: str = CharField(null=True)
    jq_l2_code: str = CharField(null=True)
    jq_l2_name: str = CharField(null=True)

    class Meta:
        database = db
        indexes = ((("index", ), True),)


if __name__ == '__main__':
    # create table
    db.connect()
    # db.create_tables([JqBarData])
    db.create_tables([JqStockInfo])
