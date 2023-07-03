from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from tabulate import tabulate

import pandas as pd
import json, pdb, requests
import threading, time, re
import datetime

from spark_monitor import Scout

def read_text(data_path):
    
    with open(data_path, 'r') as tf:
        data = tf.read().splitlines()

    return data


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
        Scout.__init__(self, self.spark, self.configs) # 將 spark session 傳入 Scout

        self.start_scout_daemon()               # -------------------> 開始監控 spark session

        sc = self.spark.sparkContext            # 間單的示範程式
        for x in range(0,5):                    # 間單的示範程式
            rdd = sc.parallelize(data)          # 間單的示範程式
            rdd = rdd.map(parser_prom_data)     # 間單的示範程式
            spdf = rdd.toDF()                   # 間單的示範程式
            spdf.show()                         # 間單的示範程式
            time.sleep(10)                      # 間單的示範程式
        self.stop_scout_daemon()                # -------------------> 停止監控
        self.join_scout_thread()                # -------------------> 回收線程
        self.spark.stop()                       # -------------------> 停止 spark session
            

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

def start_monitor(session_or_url, configs):
    scout = Scout(session_or_url, configs)

if __name__ == "__main__":

    data_path = '/root/playground/spark-monitor-Scout/code/data.txt'

    # spark configuration
    configs = {
        "configs":{
            "spark.executor.memory":"4g",
            "spark.executor.cores":"2",
            "spark.driver.memory":"2g",
            "spark.driver.cores":"2",
            "spark.executor.instances":"2"
        },
        "appName":"spark session",
        "master":"spark://spark-master:7077"
    }

    # monitor configeration
    scout_configs = {
        "guardian_type":"type-I",
        "mode": "file",
        "debug":{
            "mode":"append",
            "file_name":"../data/debug.txt"
        },
        "output":{
            "executor_info": {
                "mode":"append",
                "file_name":"../data/information_by_executor.txt",
            },
            "summary": {
                "mode":"append",
                "file_name":"../data/executors_summary.txt"
            }
        }
    }

    data = read_text(data_path)

    main = Main({**configs, **scout_configs})
    #spark_monitor = SparkMonitor(main.spark)
    #daemon_thread = threading.Thread(target=spark_monitor.start)
    #daemon_thread.daemon = True
    #daemon_thread.start()
    
    main.start(data)
    


    #while True:
    #    print("yes 112233")
    #    time.sleep(5)
