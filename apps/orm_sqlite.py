import json
from datetime import datetime, date

from peewee import (
    AutoField,
    CharField,
    Database,
    DateTimeField,
    FloatField,
    Model,
    SqliteDatabase,
    BooleanField,
    chunked,
    DateField,
    IntegerField
)

database_path = "../jq_database.db"
db = SqliteDatabase(database_path)


class JqBarData(Model):
    """聚宽历史数据表"""
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
    """聚宽股票详情表"""
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

    is_etf50: int = IntegerField(default=2)  # 是否上证50  1是 2不是
    is_if300: int = IntegerField(default=2)  # 是否沪深300
    is_csi500: int = IntegerField(default=2)  # 是否中证500
    is_science: int = IntegerField(default=2)  # 是否科创版

    class Meta:
        database = db
        indexes = ((("index", ), True),)


class StatDailyData(Model):
    """天单位统计数据表"""
    id = AutoField()

    stat_date: date = DateField(null=True)

    science_money: float = FloatField(null=True)  # 科创板 000688 成交额
    etf50_money: float = FloatField(null=True)  # 上证50 000016 成交额
    if300_money: float = FloatField(null=True)  # 沪深300 000300 成交额
    csi500_money: float = FloatField(null=True)  # 中证500 399905 成交额

    front10_money: float = FloatField(null=True)  # 当日成交额前10总和
    front15_money: float = FloatField(null=True)  # 当日成交额前15总和
    front20_money: float = FloatField(null=True)  # 当日成交额前20总和

    class Meta:
        database = db
        indexes = ((("stat_date", ), True),)


if __name__ == '__main__':
    # create table
    db.connect()
    # db.create_tables([JqBarData])
    db.create_tables([JqStockInfo])
    # db.create_tables([StatDailyData])
