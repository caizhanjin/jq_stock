import json


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
