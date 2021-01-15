"""
数据清洗脚本
"""
from datetime import datetime, date
from time import sleep

from peewee import JOIN

from utils.utility import save_json, load_json, get_trade_days
from apps.orm_sqlite import JqBarData, chunked, db, JqStockInfo, StatDailyData


def update_stock_info_to_database():
    """股票详情数据入库"""
    stocks_info = load_json("../data/stocks_info.json")
    industry_info = load_json("../data/industry_info.json")
    stocks_dict = []

    index_list = load_json("../data/index_list.json")
    etf50 = index_list.get("上证50", [])
    if300 = index_list.get("沪深300", [])
    csi500 = index_list.get("中证500", [])

    for index, item in stocks_info.items():
        index = item.get("index")

        _industry_info = industry_info.get(index, {})
        sw_l1 = _industry_info.get("sw_l1", {})
        sw_l2 = _industry_info.get("sw_l2", {})
        sw_l3 = _industry_info.get("sw_l3", {})
        zjw = _industry_info.get("zjw", {})
        jq_l1 = _industry_info.get("jq_l1", {})
        jq_l2 = _industry_info.get("jq_l2", {})

        stocks_dict.append({
            "index": item.get("index"),
            "display_name": item.get("display_name"),
            "name": item.get("name"),
            "start_date": item.get("start_date"),
            "end_date": item.get("end_date"),
            "type": item.get("type"),

            "sw_l1_code": sw_l1.get("industry_code"),
            "sw_l1_name": sw_l1.get("industry_name"),
            "sw_l2_code": sw_l2.get("industry_code"),
            "sw_l2_name": sw_l2.get("industry_name"),
            "sw_l3_code": sw_l3.get("industry_code"),
            "sw_l3_name": sw_l3.get("industry_name"),
            "zjw_code": zjw.get("industry_code"),
            "zjw_name": zjw.get("industry_name"),
            "jq_l1_code": jq_l1.get("industry_code"),
            "jq_l1_name": jq_l1.get("industry_name"),
            "jq_l2_code": jq_l2.get("industry_code"),
            "jq_l2_name": jq_l2.get("industry_name"),

            "is_etf50": 1 if index in etf50 else 2,
            "is_if300": 1 if index in if300 else 2,
            "is_csi500": 1 if index in csi500 else 2,
            "is_science": 1 if index.startswith("688") else 2,
        })

    with db.atomic():
        for c in chunked(stocks_dict, 25):
            JqStockInfo.insert_many(c).on_conflict_replace().execute()

    print(f"股票详情数据入库，总 {len(stocks_info.keys())} 条数据同步完毕...")


def update_daily_data(start_date, end_date=None):
    """daily统计数据更新"""
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    _, trade_days = get_trade_days(start_date, end_date)

    missing_days = []
    for _stat_date in trade_days:

        queryset_science = (
            JqBarData.select(JqBarData, JqStockInfo)
            .join(
                JqStockInfo,
                on=(JqBarData.index == JqStockInfo.index),
                join_type=JOIN.LEFT_OUTER
            )
            .where((JqBarData.datetime == _stat_date) & (JqStockInfo.is_science == 1))
        )
        queryset_etf50 = (
            JqBarData.select(JqBarData, JqStockInfo)
            .join(
                JqStockInfo,
                on=(JqBarData.index == JqStockInfo.index),
                join_type=JOIN.LEFT_OUTER
            )
            .where((JqBarData.datetime == _stat_date) & (JqStockInfo.is_etf50 == 1))
        )
        queryset_if300 = (
            JqBarData.select(JqBarData, JqStockInfo)
            .join(
                JqStockInfo,
                on=(JqBarData.index == JqStockInfo.index),
                join_type=JOIN.LEFT_OUTER
            )
            .where((JqBarData.datetime == _stat_date) & (JqStockInfo.is_if300 == 1))
        )
        queryset_csi500 = (
            JqBarData.select(JqBarData, JqStockInfo)
            .join(
                JqStockInfo,
                on=(JqBarData.index == JqStockInfo.index),
                join_type=JOIN.LEFT_OUTER
            )
            .where((JqBarData.datetime == _stat_date) & (JqStockInfo.is_csi500 == 1))
        )
        science_money = sum([i.money for i in queryset_science if i.money])
        etf50_money = sum([i.money for i in queryset_etf50])
        if300_money = sum([i.money for i in queryset_if300])
        csi500_money = sum([i.money for i in queryset_csi500])

        queryset = (
            JqBarData.select()
            .where((JqBarData.datetime == _stat_date))
            .order_by(JqBarData.money.desc())
            .limit(30)
        )

        front_money_list = [i.money for i in queryset]
        if len(front_money_list) == 0:
            missing_days.append(_stat_date)
            continue

        StatDailyData.insert(
            stat_date=_stat_date,
            science_money=science_money,
            etf50_money=etf50_money,
            if300_money=if300_money,
            csi500_money=csi500_money,
            front10_money=sum(front_money_list[:10]),
            front15_money=sum(front_money_list[:15]),
            front20_money=sum(front_money_list[:20]),
        ).on_conflict_replace().execute()

    print(f"daily统计数据更新完毕，缺失数据：{len(missing_days)}条 {missing_days}")


def main():
    # 更新股票详情到数据库
    # update_stock_info_to_database()

    # daily统计数据更新
    update_daily_data(start_date="2020-11-01")


if __name__ == '__main__':
    main()
    pass
