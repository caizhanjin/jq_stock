from datetime import datetime
from time import sleep

from jq_stock.utility import save_json, load_json
from jq_stock.orm_sqlite import JqBarData, chunked, db, JqStockInfo

from jqdatasdk import *


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


def save_stocks_info():
    """保存股票详情数据"""
    stocks = get_all_securities(['stock'])
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
    save_json("data/stocks_info.json", stocks_dict)
    print("股票详情数据更新完毕")


def save_industry_info():
    """保存股票行业数据"""
    stocks_info = load_json("data/stocks_info.json")
    stocks_index_list = list(stocks_info.keys())
    industry_info = get_industry(stocks_index_list, date=None)

    save_json("data/industry_info.json", industry_info)
    print("股票行业数据更新完毕")


def save_history_data(interval="1d"):
    default_start_date = "2020-10-1"

    stocks_info = load_json("data/stocks_info.json")
    stocks = list(stocks_info.keys())
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


def save_history_data2(interval="1d"):
    default_start_date = "2020-12-1"
    end_date = datetime.now().strftime("%Y-%m-%d")

    stocks_info = load_json("data/stocks_info.json")
    stocks = list(stocks_info.keys())

    history_data = get_price(
        security=stocks,
        start_date=default_start_date,
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

    print(f"总 {len(stocks)} 个，从 {default_start_date} 到 {end_date} {len(history_data_dict)} 条数据同步完毕...")


def save_stock_info_to_database():
    stocks_info = load_json("data/stocks_info.json")
    industry_info = load_json("data/industry_info.json")
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
        pass

    with db.atomic():
        for c in chunked(stocks_dict, 50):
            JqStockInfo.insert_many(c).on_conflict_replace().execute()

    print(f"总 {len(stocks_info.keys())} 条数据同步完毕...")


def main():
    # auth("18813937194", "bubiou123@J")
    # sw_dict = load_json("sw_3_info.json")
    # test_stock = sw_dict["851731"]
    # save_stocks_info()
    # save_industry_info()
    # save_history_data2()
    save_stock_info_to_database()
    pass


if __name__ == "__main__":

    main()


