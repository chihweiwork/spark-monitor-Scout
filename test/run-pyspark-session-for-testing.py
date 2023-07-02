from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from tabulate import tabulate
import re, datetime, time

import pandas as pd
import pdb
class Main:
    def __init__(self, configs):
        conf = SparkConf().setAll(configs['configs'].items())

        self.spark = SparkSession.builder\
                         .appName(configs['appName'])\
                         .master(configs['master'])\
                         .config(conf=conf)\
                         .getOrCreate()

    def start(self, data):
        sc = self.spark.sparkContext

        rdd = sc.parallelize(data)
        rdd = rdd.map(parser_prom_data)
        spdf = rdd.toDF()
        spdf.show()
        data = spdf.toPandas()

    def get_application_url(self):
        return self.spark.sparkContext.uiWebUrl
    def get_application_id(self):
        return self.spark.sparkContext.applicationId
    def get_session(self):
        return self.spark
    def stop(self):
        self.spark.close()

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

    return data

import warnings
from datetime import datetime
from typing import Any
from typing import Dict

import pandas as pd
import requests
import urlpath
import datetime
import tabulate
import sys
import threading
import traceback

from pyspark.sql import SparkSession

class Application:
    """This class is an helper to query Spark API and save historical."""
    #def __init__(self, spark_session):
    #    self.web_url = spark_session.sparkContext
    #    self.application_id = self.spark_context.applicationId
    def __init__(self, web_url, application_id, api_applications_link="api/v1/applications", debug=True) -> None:
        self.web_url = web_url
        self.application_id = application_id
        self.api_applications_link = api_applications_link
        self.debug = debug

        self.executors_db: Dict[Any, Any] = {}  # The key is timestamp
        self.timeseries_db: Dict[Any, Any] = {}  # The key is timestamp
        self.stages_df: pd.DataFrame = pd.DataFrame()
        self.tasks_db: Dict[str, Dict[Any, Any]] = {}  # The key is stageId.attemptId

    def get_executors_info(self) -> pd.DataFrame:
        """Retrieve executors info."""
        url = urlpath.URL(
                self.web_url,
                self.api_applications_link,
                self.application_id,
                "executors",
        )
        executors_df = pd.read_json(url)
        return executors_df

    def log_executors_info(self) -> None:
        """Add a new executor info in the dict database."""
        executors_df = self.get_executors_info()

        now = pd.to_datetime(datetime.datetime.now())
        if self.debug:
            self.executors_db[now] = executors_df  # Storing full row data

        self.timeseries_db[now] = self.parse_executors(executors_df)
        # Remark: local_memory_pct is a percentage, doesn't really work if you are running an kube
        #self.timeseries_db[now]["local_memory_pct"] = psutil.virtual_memory()[2]  # Local machine memory usage
        #self.timeseries_db[now]["process_memory_usage"] = get_memory_process()  # Local machine memory usage
        #self.timeseries_db[now]["user_memory_usage"] = get_memory_user()  # Local machine memory usage

    def parse_db(self) -> None:
        """Re-parse the full executors_db, usefull if you change the parsing function, for development."""
        if not self.debug:
            log.warning("parse_db should be used with debug=True")
        for t, executors_df in self.executors_db.items():
            self.timeseries_db[t].update(self.parse_executors(executors_df))

    @staticmethod
    def parse_executors(executors_df: pd.DataFrame) -> Dict[Any, Any]:
        """Convert an executors DataFrame to a dictionnary.

        More spefically we aggregate metrix from all executors into one.
        """
        if len(executors_df) == 0:
            return {}

        memoryMetrics = executors_df["memoryMetrics"].dropna()
        memoryMetrics_df = pd.DataFrame.from_records(memoryMetrics, index=memoryMetrics.index)
        executors_df = executors_df.join(memoryMetrics_df)

        if "peakMemoryMetrics" in executors_df.columns:
            peakMemoryMetrics = executors_df["peakMemoryMetrics"].dropna()
            peakMemoryMetrics_df = pd.DataFrame.from_records(peakMemoryMetrics, index=peakMemoryMetrics.index)
            executors_df = executors_df.join(peakMemoryMetrics_df)

        executors_df["usedOnHeapStorageMemoryPct"] = (
            executors_df["usedOnHeapStorageMemory"] / executors_df["totalOnHeapStorageMemory"] * 100
        )
        executors_df["usedOffHeapStorageMemoryPct"] = (
            executors_df["usedOffHeapStorageMemory"] / executors_df["totalOffHeapStorageMemory"] * 100
        )
        executors_df["memoryUsedPct"] = executors_df["memoryUsed"] / executors_df["maxMemory"] * 100

        # Columns to aggregate
        cols = [
            "memoryUsedPct",
            "usedOnHeapStorageMemoryPct",
            "usedOffHeapStorageMemoryPct",
            "totalOnHeapStorageMemory",
            "totalOffHeapStorageMemory",
            "ProcessTreePythonVMemory",
            "ProcessTreePythonRSSMemory",
            "JVMHeapMemory",
            "JVMOffHeapMemory",
            "OffHeapExecutionMemory",
            "OnHeapExecutionMemory",
        ]

        # Filter columns that are not on the DataFrame
        cols = [col for col in cols if col in executors_df.columns]

        # Obtain aggregated values
        # Let's filter: "RuntimeWarning: Mean of empty slice"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            res = executors_df[cols].agg(["max", "mean", "median", "min"])

        # Convert to dict
        d: Dict[str, Any] = {
            f"{key}_{agg}": value for key, values in res.to_dict().items() for agg, value in values.items()
        }

        d["numActive"] = len(executors_df.query("isActive"))
        d["memoryUsed_sum"] = executors_df["memoryUsed"].sum()
        d["maxMemory_sum"] = executors_df["maxMemory"].sum()
        d["memoryUsed_sum_pct"] = executors_df["memoryUsed"].sum() / executors_df["maxMemory"].sum() * 100
        d["memoryUsedPct_driver"] = executors_df.iloc[0]["memoryUsedPct"]

        return d

    def log_stages(self) -> None:
        """Retrieve stages."""
        url = urlpath.URL(self.web_url, self.api_applications_link, self.application_id, "stages")
        self.stages_df = pd.read_json(url)

    def log_tasks(self) -> None:
        """Retrieve tasks."""
        for _, row in self.stages_df.iterrows():
            stage_uid = f"{row['stageId']}.{row['attemptId']}"

            # Initialize
            if self.tasks_db.get(stage_uid) is None:
                self.tasks_db[stage_uid] = {"tasks": None, "stage_last_status": None}

            # Don't query again what is done
            if self.tasks_db[stage_uid]["stage_last_status"] in ["COMPLETE", "SKIPPED", "FAILED"]:
                continue

            url = urlpath.URL(
                self.web_url,
                self.api_applications_link,
                self.application_id,
                "stages",
                str(row["stageId"]),
                str(row["attemptId"]),
            )

            stage_detail_r = requests.get(url)

            # We some time get this exception: simplejson.errors.JSONDecodeError: Expecting value: line 1 column 1 (char 0)
            stage_detail = stage_detail_r.json()

            tasks = stage_detail["tasks"]

            # Flatten the dictionnary
            tasks = [flatten_dict(tasks[k]) for k in tasks.keys()]

            self.tasks_db[stage_uid]["tasks"] = tasks
            self.tasks_db[stage_uid]["stage_last_status"] = row["status"]

    def get_tasks_df(self) -> pd.DataFrame:
        """Return all tasks info into a DataFrame."""
        tasks_list = []
        # Iter stages
        for k in self.tasks_db.keys():
            tasks = self.tasks_db[k]["tasks"]
            # Iter tasks
            for t in tasks:
                t["stage_uid"] = k
                tasks_list.append(t)
        tasks_df = pd.DataFrame(tasks_list)
        return tasks_df

    def get_timeseries_db_df(self) -> pd.DataFrame:
        """Return timeseries_db info into a DataFrame."""
        timeseries_db_df = pd.DataFrame(self.timeseries_db).T
        timeseries_db_df.index.rename("timestamp", inplace=True)
        return timeseries_db_df

    def log_all(self) -> None:
        """Updating all information."""
        self.log_executors_info()
        self.log_stages()
        self.log_tasks()

def get_application_ids(web_url: str, api_applications_link: str="api/v1/applications") -> pd.DataFrame:
    """Retrieve available application id."""
    applications_df = pd.read_json(urlpath.URL(web_url, API_APPLICATIONS_LINK))
    log.info(f"applications_df:\n{applications_df}")
    return applications_df

def create_application_by_url(web_url: str, index: int = 0) -> Application:
    """Create an Application.

    :param index: Application index in the application list
    :param web_url: Spark REST API server
    """
    applications_df = get_application_ids(web_url)
    application_id = applications_df["id"].iloc[index]

    application = Application(application_id, web_url)

    return application

def create_application_from_session(spark: SparkSession) -> Application:
    """Create an Application from Spark Session."""
    web_url = spark.sparkContext.uiWebUrl
    application_id = spark.sparkContext.applicationId
    
    application = Application(web_url, application_id)
    return application
    
class SparkMonitor(threading.Thread):
    """This class is an executor to monitor spark application."""
    def __init__(self, session_or_url):
        super().__init__()
        self.daemon = True
        self.stop_flag = threading.Event()
        try:
            if isinstance(session_or_url, str):
                self.app = create_application_by_url(session_or_url)
            elif isinstance(session_or_url, SparkSession):
                self.app = create_application_from_session(session_or_url)
            else: 
                raise TypeError(f"First argument `application_or_spark` is unsupported type {type(application_or_spark)}")
        except Exception as err:
           err_type = err.__class__.__name__ # 取得錯誤的class 名稱
           info = err.args[0] # 取得詳細內容
           detains = traceback.format_exc() # 取得完整的tracestack
           n1, n2, n3 = sys.exc_info() #取得Call Stack
           lastCallStack =  traceback.extract_tb(n3)[-1] # 取得Call Stack 最近一筆的內容
           fn = lastCallStack [0] # 取得發生事件的檔名
           lineNum = lastCallStack[1] # 取得發生事件的行數
           funcName = lastCallStack[2] # 取得發生事件的函數名稱
           errMesg = f"FileName: {fn}, lineNum: {lineNum}, Fun: {funcName}, reason: {info}, trace:\n {traceback.format_exc()}"
           print(errMesg)
           sys.exit()

    def run(self):
        while not self.stop_flag.is_set():
            print("run something ...")
            self.app.log_all()
            self.app.get_timeseries_db_df().to_csv('./data.csv')
            
            time.sleep(15)

    def stop(self):
        self.app.log_all()
        self.app.get_timeseries_db_df().to_csv('./data.csv')
        self.stop_flag.set() 


if __name__ == "__main__":

    data_path = '/root/playground/spark-monitor-test/code/data.txt'

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

    data = read_text(data_path)

    while True:
        main = Main(configs)
        #pdb.set_trace()
        session = main.get_session()
        sm = SparkMonitor(session)
        sm.run()
        #create_application_from_session(session)
        main.start(data+data+data)
        main.stop()
        sm.stop()
        time.sleep(10)
