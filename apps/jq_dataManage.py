"""
聚宽数据同步脚本
"""
from datetime import datetime
from time import sleep

from utils.utility import save_json, load_json
from apps.orm_sqlite import JqBarData, chunked, db, JqStockInfo

from jqdatasdk import *

# 登录聚宽账号
auth("", "")


def save_sw_three():
    """申万三级数据"""
    sw_list = get_industries(name="sw_l3", date=None)
    sw_dict = {}
    for index, row in sw_list.iterrows():
        sw_dict[index] = {
            "index": index,
            "name": row["name"],
            "start_date": row["start_date"].strftime("%Y-%m-%d %H:%M:%S"),
            "stocks": get_industry_stocks(index)
        }

    save_json("data/sw.json", sw_dict)
    print("申万三级数据更新完毕")


def save_stocks_info(stocks=None):
    """保存股票详情数据"""
    stocks = stocks if stocks else get_all_securities(['stock'])
    stocks_dict = {}
    for index, row in stocks.iterrows():
        stocks_dict[index] = {
            "index": index,
            "display_name": row["display_name"],
            "name": row["name"],
            "start_date": row["start_date"].strftime("%Y-%m-%d %H:%M:%S"),
            "end_date": row["end_date"].strftime("%Y-%m-%d %H:%M:%S"),
            "type": row["type"],
        }
    save_json("../data/stocks_info.json", stocks_dict)
    print("股票详情数据更新完毕")


def save_industry_info():
    """保存股票行业数据"""
    stocks_info = load_json("../data/stocks_info.json")
    stocks_index_list = list(stocks_info.keys())
    industry_info = get_industry(stocks_index_list, date=None)

    save_json("../data/industry_info.json", industry_info)
    print("股票行业数据更新完毕")


def save_history_data(stock=None, stocks=None, interval="1d"):
    """同步指定股票最新数据"""
    print("开始同步最新数据...")
    default_start_date = "2020-10-1"

    if stock is not None:
        stocks = [stock]

    stocks_count = len(stocks)
    sync_num = 1

    for stock in stocks:
        newest_bar = (
            JqBarData.select()
            .where((JqBarData.index == stock) & (JqBarData.interval == interval))
            .order_by(JqBarData.datetime.desc())
            .first()
        )

        if newest_bar:
            start_date = newest_bar.datetime.strftime("%Y-%m-%d")
        else:
            start_date = default_start_date
        end_date = datetime.now().strftime("%Y-%m-%d")

        history_data = get_price(
            stock,
            start_date=start_date,
            end_date=end_date,
            frequency=interval,
            fields=['open', 'close', 'low', 'high', 'volume', 'money',
                    'factor', 'high_limit', 'low_limit', 'avg', 'pre_close', 'paused'],
            skip_paused=False,
            fq='pre',
            count=None,
            panel=False,
            fill_paused=True
        )
        history_data_dict = []
        for index, row in history_data.iterrows():
            history_data_dict.append({
                "index": stock,
                "datetime": index.strftime("%Y-%m-%d %H:%M:%S"),
                "interval": interval,

                "open": row["open"],
                "close": row["close"],
                "low": row["low"],
                "high": row["high"],
                "volume": row["volume"],
                "money": row["money"],
                "factor": row["factor"],
                "high_limit": row["high_limit"],
                "low_limit": row["low_limit"],
                "avg": row["avg"],
                "pre_close": row["pre_close"],
                "paused": row["paused"]
            })

        print(f"总 {stocks_count}，开始同步第 {sync_num} 个 {stock}，从 {start_date} 到 {end_date} {len(history_data_dict)} 条数据...")
        with db.atomic():
            for c in chunked(history_data_dict, 50):
                JqBarData.insert_many(c).on_conflict_replace().execute()

        sync_num += 1
        sleep(1)


def save_history_data2(start_date="2020-12-1", end_date=None, stocks=None, interval="1d"):
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if stocks is None:
        stocks_info = load_json("../data/stocks_info.json")
        stocks = list(stocks_info.keys())

    history_data = get_price(
        security=stocks,
        start_date=start_date,
        end_date=end_date,
        frequency=interval,
        fields=['open', 'close', 'low', 'high', 'volume', 'money',
                'factor', 'high_limit', 'low_limit', 'avg', 'pre_close', 'paused'],
        skip_paused=False,
        fq='pre',
        count=None,
        panel=False,
        fill_paused=True
    )
    print("数据下载完毕，开始同步...")

    history_data_dict = []
    for index, row in history_data.iterrows():
        history_data_dict.append({
            "index": row["code"],
            "datetime": row["time"].strftime("%Y-%m-%d %H:%M:%S"),
            "interval": interval,

            "open": row["open"],
            "close": row["close"],
            "low": row["low"],
            "high": row["high"],
            "volume": row["volume"],
            "money": row["money"],
            "factor": row["factor"],
            "high_limit": row["high_limit"],
            "low_limit": row["low_limit"],
            "avg": row["avg"],
            "pre_close": row["pre_close"],
            "paused": row["paused"]
        })

    with db.atomic():
        for c in chunked(history_data_dict, 50):
            JqBarData.insert_many(c).on_conflict_replace().execute()

    print(f"总 {len(stocks)} 个，从 {start_date} 到 {end_date} {len(history_data_dict)} 条数据同步完毕...")


def save_index_stocks(index, meaning):
    """更新指数股票列表"""
    index_list = load_json("../data/index_list.json")

    stocks = get_index_stocks(index)
    index_list[meaning] = stocks

    save_json("../data/index_list.json", index_list)
    print(f"指数 {index} 股票列表更新完毕")


def main():
    # sw_dict = load_json("sw_3_info.json")
    # test_stock = sw_dict["851731"]

    # 基础数据准备
    save_stocks_info()  # 保存股票详情
    save_industry_info()  # 保存行业基础数据

    # 同步指定股票最新数据
    # save_history_data(stock="000012.XSHE")
    # 批量更新股票历史数据(所有)
    save_history_data2(start_date="2020-12-01", end_date=None)
    # save_history_data2(start_date="2020-11-15", end_date="2020-12-01")

    # 更新指数股票列表
    save_index_stocks("000016.XSHG", "上证50")
    save_index_stocks("000905.XSHG", "中证500")
    save_index_stocks("000300.XSHG", "沪深300")

    pass


if __name__ == "__main__":

    main()



