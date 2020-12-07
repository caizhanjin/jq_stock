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

    for index, item in stocks_info.items():
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
        })

    with db.atomic():
        for c in chunked(stocks_dict, 50):
            JqStockInfo.insert_many(c).on_conflict_replace().execute()

    print(f"股票详情数据入库，总 {len(stocks_info.keys())} 条数据同步完毕...")


def update_daily_data(start_date, end_date=None):
    """daily统计数据更新"""
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    _, trade_days = get_trade_days(start_date, end_date)

    missing_days = []
    for _stat_date in trade_days:
        queryset = (
            JqBarData.select()
            .where((JqBarData.datetime == _stat_date))
            .order_by(JqBarData.money.desc())
            .limit(30)
        )

        money_list = [i.money for i in queryset]

        if len(money_list) == 0:
            missing_days.append(_stat_date)
            continue

        StatDailyData.insert(
            stat_date=_stat_date,
            front10_money=sum(money_list[:10]),
            front15_money=sum(money_list[:15]),
            front20_money=sum(money_list[:20]),
        ).on_conflict_replace().execute()

    print(f"daily统计数据更新完毕，缺失数据：{len(missing_days)}条 {missing_days}")


def main():
    # 更新股票详情到数据库
    # update_stock_info_to_database()

    # daily统计数据更新
    update_daily_data(start_date="2020-12-01")


if __name__ == '__main__':
    main()
    pass
