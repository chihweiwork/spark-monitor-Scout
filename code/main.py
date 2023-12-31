from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from tabulate import tabulate

import pandas as pd
import json
import pdb
import requests
import threading
import time
import datetime
import random

from spark_monitor import Scout
from tool import exception_info

import re
import datetime

def parser_prom_data(row):
    pattern=r"{(?P<data>.*)} (?P<value>(\S+)) (?P<timestamp>(\S+))"
    tmp = re.match(pattern, row).groupdict()

    output = {key:value for key, value in gen_labels(tmp['data'])}
    output['value'] = tmp['value']
    output['timestamp'] = int(tmp['timestamp'])//1000
    output['date'] = get_datetime_string(output['timestamp'])
    return output

def gen_labels(labels: str):
    # 解析 labels
    # Input:
    # __name__="scrape_series_added", instance="172.29.1.3:9100", job="worknode"

    targets = labels.replace('"','').replace(' ','')
    for target in targets.split(','):
        key, value = target.split('=')
        if key == '__name__':
            yield "metric", value
        else:
            yield key, value

def get_datetime_string(target: int) -> str:
    # 時間戳記轉換成 string 格式為 yyyy-mm-dd
    # Input:
    #   timestamp
    # output:
    #   yyyy-mm-dd
    tmp = datetime.datetime.fromtimestamp(target)
    return tmp.strftime('%Y-%m-%d')

def read_text(data_path):
    
    with open(data_path, 'r') as tf:
        data = tf.read().splitlines()
    num = random.randint(1,617900)

    return data[:num]


class Main(Scout): # 繼承 Scout
    """
    使用 Scout 的範例
    """
    def __init__(self, configs):
        # 取得 configs 並且將 spark 相關設定值設定上去
        self.conf = SparkConf().setAll(configs['configs'].items())
        self.configs = configs

    def set_spark_session(self):
        """
        設定 spark session
        """
        self.spark = SparkSession.builder\
                         .appName(self.configs['appName'])\
                         .master(self.configs['master'])\
                         .config(conf=self.conf)\
                         .getOrCreate()

    def start(self, data):
        self.set_spark_session() # 先初始化 spark session

        Scout.__init__(self, self.spark, self.configs) # -------------------> 將 spark session 傳入 Scout
        self.start_scout_daemon()                      # -------------------> 開始監控 spark session

        sc = self.spark.sparkContext                   # 間單的示範程式
        while True:
            rdd = sc.parallelize(data)                 # 間單的示範程式
            rdd = rdd.map(parser_prom_data)            # 間單的示範程式
            spdf = rdd.toDF()                          # 間單的示範程式
            spdf.show()                                # 間單的示範程式
            time.sleep(10)                             # 間單的示範程式

        self.stop_scout_daemon()                       # -------------------> 停止監控
        self.join_scout_thread()                       # -------------------> 回收線程

        self.spark.stop()                              # -------------------> 停止 spark session
            

if __name__ == "__main__":

    data_path = '/root/playground/spark-monitor-Scout/code/data.txt'

    # spark configuration
    configs = {
        "configs":{
            "spark.driver.memory":"1g",
            "spark.driver.cores":"1",
            "spark.dynamicAllocation.enabled":"true",
            "spark.executor.cores":2,
            "spark.dynamicAllocation.minExecutors":"1",
            "spark.dynamicAllocation.maxExecutors":"2"
        },
        "appName":"spark session",
        "master":"spark://spark-master:7077"
    }

    # monitor configeration
    timestamp = f"{int(time.time())}"
    scout_configs = {
        "guardian_type":["executor_monitor"],
        "mode": "file",
        "time_interval":15, 
        "log_path":"../data",
        "debug":{
            "mode":"append"
        },
        "info":{
            "mode":"append"
        },
        "summary":{
            "mode":"append"
        }
    }

    data = read_text(data_path)

    main = Main({**configs, **scout_configs})

    main.start(data)
