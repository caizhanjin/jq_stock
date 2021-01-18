"""
数据分析统计脚本
"""
from datetime import datetime
import time
from apps.orm_sqlite import JqBarData, JqStockInfo, StatDailyData
from peewee import JOIN
import pandas as pd
import os
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

from utils.utility import load_json, get_trade_days


def reset_col(filename):
    wb = load_workbook(filename)
    for sheet in wb.sheetnames:
        ws = wb[sheet]
        df = pd.read_excel(filename, sheet, engine="openpyxl").fillna('-')
        df.loc[len(df)] = list(df.columns)
        for col in df.columns:
            index = list(df.columns).index(col)
            letter = get_column_letter(index+1)
            collen = df[col].apply(lambda x: len(str(x).encode()))[1:].max()
            ws.column_dimensions[letter].width = max(min(collen*1.2, 16), 8)

    wb.save(filename)


def main(_start_date, _end_date=None):
    """
    统计脚本（主）
    """
    print(f"开始统计从 {_start_date} 到 {_end_date} 数据...")
    start_time = time.time()
    if _end_date is None:
        _end_date = datetime.now().strftime("%Y-%m-%d")

    _, trade_days = get_trade_days(_start_date, _end_date)

    bar_list = (
        JqBarData.select(JqBarData, JqStockInfo)
        .join(
            JqStockInfo,
            on=(JqBarData.index == JqStockInfo.index),
            join_type=JOIN.LEFT_OUTER
        )
        .where((JqBarData.datetime in trade_days) & (JqStockInfo.type == "stock"))
        .order_by(JqBarData.money.desc())
    )
    bar_dict = []

    for bar in bar_list:
        # 涨幅
        if bar.close and bar.pre_close:
            change_percent = round((bar.close - bar.pre_close) / bar.pre_close * 100, 2)
        else:
            change_percent = 0

        bar_dict.append({
            "code": bar.index,
            "display_name": "" if not bar.jqstockinfo else bar.jqstockinfo.display_name,
            "change_percent": change_percent,
            "money": 0 if not bar.money else bar.money,
            "datetime": bar.datetime,
            # "interval": bar.interval,

            "open": bar.open,
            "close": bar.close,
            "low": bar.low,
            "high": bar.high,
            "volume": bar.volume,
            "pre_close": bar.pre_close,

            "zjw_name": "" if not bar.jqstockinfo else bar.jqstockinfo.zjw_name,
            "sw_l1_name": "" if not bar.jqstockinfo else bar.jqstockinfo.sw_l1_name,
            "sw_l2_name": "" if not bar.jqstockinfo else bar.jqstockinfo.sw_l2_name,
            "sw_l3_name": "" if not bar.jqstockinfo else bar.jqstockinfo.sw_l3_name,

            "is_etf50": "否" if not bar.jqstockinfo else ("是" if bar.jqstockinfo.is_etf50 == 1 else "否"),
            "is_if300": "否" if not bar.jqstockinfo else ("是" if bar.jqstockinfo.is_if300 == 1 else "否"),
            "is_csi500": "否" if not bar.jqstockinfo else ("是" if bar.jqstockinfo.is_csi500 == 1 else "否"),
            "is_science": "否" if not bar.jqstockinfo else ("是" if bar.jqstockinfo.is_science == 1 else "否"),
        })

    all_df = pd.DataFrame.from_dict(bar_dict, orient="columns")
    all_df.sort_values(by=["money"], ascending=[False], inplace=True)

    count_stat_list = []
    volume_stat_list = []
    for trade_day in trade_days:
        _df = all_df[(all_df["datetime"] == trade_day + " 00:00:00")]

        df_1 = _df[(_df['money'] >= 100000000.0)]
        df_2 = _df[(_df['money'] >= 200000000.0)]
        df_20 = _df[_df["money"] >= 2000000000.0]

        count_stat_list.append({
            "datetime": trade_day,

            "大于1亿总数": df_1.shape[0],
            "大于1亿上涨数": df_1[(df_1['change_percent'] > 0)].shape[0],
            "大于1亿下跌数": df_1[(df_1['change_percent'] == 0)].shape[0],
            "大于1亿持平数": df_1[(df_1['change_percent'] < 0)].shape[0],

            "大于2亿总数": df_2.shape[0],
            "大于2亿上涨数": df_2[(df_2['change_percent'] > 0)].shape[0],
            "大于2亿下跌数": df_2[(df_2['change_percent'] == 0)].shape[0],
            "大于2亿持平数": df_2[(df_2['change_percent'] < 0)].shape[0],

            "大于20亿总数": df_20.shape[0],
            "大于20亿上涨数": df_20[(df_20['change_percent'] > 0)].shape[0],
            "大于20亿下跌数": df_20[(df_20['change_percent'] == 0)].shape[0],
            "大于20亿持平数": df_20[(df_20['change_percent'] < 0)].shape[0],
        })

        volume_stat_list.append({
            "datetime": trade_day,

            "science_money": _df[(_df['is_science'] == '是')]['money'].sum(),
            "etf50_money": _df[(_df['is_etf50'] == '是')]['money'].sum(),
            "if300_money": _df[(_df['is_if300'] == '是')]['money'].sum(),
            "csi500_money": _df[(_df['is_csi500'] == '是')]['money'].sum(),

            "front10_money": _df[:10]['money'].sum(),
            "front15_money": _df[:15]['money'].sum(),
            "front20_money": _df[:20]['money'].sum(),
        })

    stat_date = trade_days[-1]
    df = all_df[(all_df["datetime"] == stat_date + " 00:00:00")]
    df = df.set_index(["code"])
    df = df.drop(['datetime'], axis=1)

    df_1 = df[(df['money'] >= 100000000.0)]
    df_2 = df[(df['money'] >= 200000000.0)]
    df_20 = df[df["money"] >= 2000000000.0]
    # excel_writer初始化
    excel_name = f"{stat_date}_" \
                 f"{df_20.shape[0]}_" \
                 f"{df_2.shape[0]}_" \
                 f"{df_1.shape[0]}_" \
                 f"{int(df[(df['is_etf50'] == '是')]['money'].sum())}_" \
                 f"{int(df[(df['is_if300'] == '是')]['money'].sum())}_" \
                 f"{int(df[(df['is_csi500'] == '是')]['money'].sum())}_" \
                 f"{int(df[(df['is_science'] == '是')]['money'].sum())}" \
                 f".xlsx"
    excel_name_path = os.path.join("output_data", excel_name)
    excel_writer = pd.ExcelWriter(excel_name_path)

    # 成交前100、200涨跌家数和区间统计
    change_dict = {}
    change_overall_list = []
    df_dict = {
        "前100": df[:100],
        "前200": df[:200],
        "大于1亿": df_1,
        "大于2亿": df_2,
        "大于20亿": df_20,
    }

    for key, item in df_dict.items():
        change_df = df_dict[key]

        change_overall_list.append({
            "stat_type": key,
            "count": change_df.shape[0],
            "up": change_df[(change_df["change_percent"] > 0)].shape[0],
            "down": change_df[(change_df["change_percent"] < 0)].shape[0],
            "stay": change_df[(change_df["change_percent"] == 0)].shape[0],
        })
        change_dict.setdefault(key, {})
        change_dict[key] = {
            "overall": {
                "count": change_df.shape[0],
                "up": change_df[(change_df["change_percent"] > 0)].shape[0],
                "down": change_df[(change_df["change_percent"] < 0)].shape[0],
                "stay": change_df[(change_df["change_percent"] == 0)].shape[0],
            },
            "info": {
                "涨停": change_df[(change_df["change_percent"] >= 9.9)].shape[0],
                "7~9.9%": change_df[(change_df["change_percent"] >= 7) & (change_df["change_percent"] < 9.9)].shape[0],
                "5~7%": change_df[(change_df["change_percent"] >= 5) & (change_df["change_percent"] < 7)].shape[0],
                "2~5%": change_df[(change_df["change_percent"] >= 2) & (change_df["change_percent"] < 5)].shape[0],
                "0~2%": change_df[(change_df["change_percent"] > 0) & (change_df["change_percent"] < 2)].shape[0],
                "平": change_df[(change_df["change_percent"] == 0)].shape[0],
                "-0~2%": change_df[(change_df["change_percent"] > -2) & (change_df["change_percent"] < 0)].shape[0],
                "-2~5%": change_df[(change_df["change_percent"] > -5) & (change_df["change_percent"] <= -2)].shape[0],
                "-5~7%": change_df[(change_df["change_percent"] > -7) & (change_df["change_percent"] <= -5)].shape[0],
                "-7~9.9%": change_df[(change_df["change_percent"] > -9.9) & (change_df["change_percent"] <= -7)].shape[0],
                "跌停": change_df[(change_df["change_percent"] <= -9.9)].shape[0],
            }
        }
        print(f"\n{key}涨跌家数和区间统计>>\n"
              f"总览：{change_dict[key]['overall']}\n"
              f"详情：{change_dict[key]['info']}")

    df_change = pd.DataFrame.from_dict(change_overall_list, orient="columns")
    df_change.set_index(["stat_type"], inplace=True)
    df_change.to_excel(excel_writer, stat_date)

    # daily_data数据查询和导出
    df_daily = pd.DataFrame.from_dict(volume_stat_list, orient="columns")
    df_daily.set_index(["datetime"], inplace=True)
    df_daily.to_excel(excel_writer, "daily_volume")
    df_daily = pd.DataFrame.from_dict(count_stat_list, orient="columns")
    df_daily.set_index(["datetime"], inplace=True)
    df_daily.to_excel(excel_writer, "daily_count")

    df.to_excel(excel_writer, "All")  # 所有数据写入
    df = df.sort_values(["sw_l1_name", "sw_l2_name", "sw_l3_name"], ascending=[1, 1, 1])
    df_1 = df[(df['money'] >= 100000000.0)]
    df_2 = df[(df['money'] >= 200000000.0)]
    df_20 = df[df["money"] >= 2000000000.0]
    df_1.to_excel(excel_writer, f"1亿以上")
    df_2.to_excel(excel_writer, f"2亿以上")
    df_20.to_excel(excel_writer, f"20亿以上")

    # 00000000.0
    money_stat = {}
    _df = df[(df["money"] >= 10000000000.0)]
    money_stat["100亿以上"] = _df.shape[0]
    _df.to_excel(excel_writer, f"100亿以上")

    _df = df[(df["money"] >= 8000000000.0) & (df["money"] < 10000000000.0)]
    money_stat["80(含)~100亿"] = _df.shape[0]
    _df.to_excel(excel_writer, f"80(含)~100亿")

    _df = df[(df["money"] >= 5000000000.0) & (df["money"] < 8000000000.0)]
    money_stat["50(含)~80亿"] = _df.shape[0]
    _df.to_excel(excel_writer, f"50(含)~80亿")

    _df = df[(df["money"] >= 5000000000.0) & (df["money"] < 8000000000.0)]
    money_stat["50(含)~80亿"] = _df.shape[0]
    _df.to_excel(excel_writer, f"50(含)~80亿")

    _df = df[(df["money"] >= 4000000000.0) & (df["money"] < 5000000000.0)]
    money_stat["40(含)~50亿"] = _df.shape[0]
    _df.to_excel(excel_writer, f"40(含)~50亿")

    _df = df[(df["money"] >= 3000000000.0) & (df["money"] < 4000000000.0)]
    money_stat["30(含)~40亿"] = _df.shape[0]
    _df.to_excel(excel_writer, f"30(含)~40亿")

    _df = df[(df["money"] >= 2000000000.0) & (df["money"] < 3000000000.0)]
    money_stat["20(含)~30亿"] = _df.shape[0]
    _df.to_excel(excel_writer, f"20(含)~30亿")

    print(f"\n成交额区间划分：{money_stat}")

    excel_writer.save()
    reset_col(excel_name_path)

    print(f"\n完成统计，累计耗时 {round(time.time() - start_time, 2)} s...")
    pass


if __name__ == '__main__':

    start_date = "2020-12-01"
    end_date = "2020-12-01"
    main(start_date, end_date)

    pass
