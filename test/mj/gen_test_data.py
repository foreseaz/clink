#!python
# coding=utf-8

import json
import random
import time

TRANS_BRAN_CODE_LIST = (
    "11670101", "11670102", "11670103", "11670104", "11670105", "11670106", "11670107", "11670108", "11670109",
    "11670110"
)

MC_TRS_CODE_LIST = ("INQ", "LIS", "CWD", "CDP", "TFR", "PIN", "REP", "PAY", "XXX", "YYY", "ZZZ")

start_time = 946656000  # 2000-01-01 00:00:00

with open("./mj_msg_1000_test.txt", "w") as data:
    for i in range(1000):
        # 步长 1min 增长
        cur_time = start_time + i * 60
        body_insert = {
            "after": {
                "TANS_AMT": "100.01",
                "TRANS_FLAG": "P",
                "TRANS_DATE": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cur_time)),
                "TRANS_BRAN_CODE": random.choice(TRANS_BRAN_CODE_LIST),
                "MC_TRSCODE": random.choice(MC_TRS_CODE_LIST),
            },
            "rowid": str(i),
            "scntime": cur_time,
            "optype": "INSERT",
            "name": "mj",
        }

        body_update = {
            "rowid": str(i),
            "scntime": cur_time + 1,
            "optype": "UPDATE",
            "name": "mj",
            "after": {
                "TRANS_FLAG": "0"
            },
            "before": {
                "TRANS_FLAG": "p"
            }
        }
        data.write(json.dumps(body_insert) + "\n")
        data.write(json.dumps(body_update) + "\n")
