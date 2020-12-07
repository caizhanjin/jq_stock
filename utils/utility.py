import json
from datetime import datetime, timedelta


def save_json(filepath, data):

    with open(filepath, mode="w+", encoding="UTF-8") as f:
        json.dump(
            data,
            f,
            indent=4,
            ensure_ascii=False
        )


def load_json(filepath):
    with open(filepath, mode="r", encoding="UTF-8") as f:
        data = json.load(f)
    return data


def get_trade_days(start_date, end_date):
    """计算两个日期间的工作日"""
    from chinese_calendar import is_holiday

    # 字符串格式日期的处理
    if type(start_date) == str:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if type(end_date) == str:
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    # 开始日期大，颠倒开始日期和结束日期
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    date_list = []
    counts = 0
    while True:
        if start_date > end_date:
            break

        if is_holiday(start_date) or start_date.weekday() == 5 or start_date.weekday() == 6:
            start_date += timedelta(days=1)
            continue

        date_list.append(str(start_date.strftime("%Y-%m-%d")))
        counts += 1
        start_date += timedelta(days=1)

    return counts, date_list


if __name__ == '__main__':
    print(get_trade_days(start_date="2020-12-01", end_date="2020-12-15"))

    pass
