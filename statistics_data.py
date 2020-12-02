from jq_stock.orm_sqlite import JqBarData, JqStockInfo
from peewee import JOIN
import pandas as pd


def test():

    date = "2020-12-01 00:00:00"

    bar_list = (
        JqBarData.select(JqBarData, JqStockInfo)
        .join(
            JqStockInfo,
            on=(JqBarData.index == JqStockInfo.index),
            join_type=JOIN.LEFT_OUTER
        )
        .where((JqBarData.datetime == date) & (JqStockInfo.type == "stock"))
        .order_by(JqBarData.money.desc())
    )
    bar_dict = []
    # change_dict = {
    #     "base": {
    #         "count": 0,
    #         "up": {"count": 0, "list": []},
    #         "down": {"count": 0, "list": []},
    #         "stay": {"count": 0, "list": []},
    #     },
    #
    # }

    for bar in bar_list:
        # 涨幅
        if bar.close and bar.pre_close:
            change_percent = round((bar.close - bar.pre_close) / bar.pre_close * 100, 2)
        else:
            change_percent = 0

        # change_dict["base"]["count"] += 1
        # if change_percent > 0:
        #     change_dict["base"]["up"]["count"] += 1
        #     change_dict["base"]["up"]["list"].append(bar.index)
        #
        # elif change_percent < 0:
        #     change_dict["base"]["down"]["count"] += 1
        #     change_dict["base"]["down"]["list"].append(bar.index)
        #
        # else:
        #     change_dict["base"]["stay"]["count"] += 1
        #     change_dict["base"]["stay"]["list"].append(bar.index)

        bar_dict.append({
            "index": bar.index,
            "datetime": bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
            "interval": bar.interval,

            "open": bar.open,
            "close": bar.close,
            "low": bar.low,
            "high": bar.high,
            "volume": bar.volume,
            "money": bar.money,
            "pre_close": bar.pre_close,
            "change_percent": change_percent,

            "display_name": "" if not bar.jqstockinfo else bar.jqstockinfo.display_name,
            "zjw_name": "" if not bar.jqstockinfo else bar.jqstockinfo.zjw_name,
            "sw_l1_name": "" if not bar.jqstockinfo else bar.jqstockinfo.sw_l1_name,
            "sw_l2_name": "" if not bar.jqstockinfo else bar.jqstockinfo.sw_l2_name,
            "sw_l3_name": "" if not bar.jqstockinfo else bar.jqstockinfo.sw_l3_name,

        })
    # , columns = [
    #     "index", "datetime", "interval", "open", "close", "low",
    #     "high", "volume", "money", "pre_close", "change_percent",
    #     "display_name", "zjw_name", "sw_l1_name", "sw_l2_name", "sw_l3_name"]
    df = pd.DataFrame.from_dict(bar_dict, orient="columns")

    change_dict = {
        "base": {
            "count": df.shape[0],
            "up": df[(df["change_percent"] > 0)].shape[0],
            "down": df[(df["change_percent"] < 0)].shape[0],
            "stay": df[(df["change_percent"] == 0)].shape[0],
            "info": {
                "涨停": df[(df["change_percent"] >= 10)].shape[0],
                "7~10%": df[(df["change_percent"] >= 7) & (df["change_percent"] < 10)].shape[0],
                "5~7%": df[(df["change_percent"] >= 5) & (df["change_percent"] < 7)].shape[0],
                "2~5%": df[(df["change_percent"] >= 2) & (df["change_percent"] < 5)].shape[0],
                "0~2%": df[(df["change_percent"] > 0) & (df["change_percent"] < 2)].shape[0],
                "平": df[(df["change_percent"] == 0)].shape[0],
                "-0~2%": df[(df["change_percent"] > -2) & (df["change_percent"] < 0)].shape[0],
                "-2~5%": df[(df["change_percent"] > -5) & (df["change_percent"] <= -2)].shape[0],
                "-5~7%": df[(df["change_percent"] > -7) & (df["change_percent"] <= -5)].shape[0],
                "-7~10%": df[(df["change_percent"] > -10) & (df["change_percent"] <= -7)].shape[0],
                "跌停": df[(df["change_percent"] <= -10)].shape[0],
            },
        },

    }

    print(change_dict["base"])
    pass


if __name__ == '__main__':
    test()
    pass
