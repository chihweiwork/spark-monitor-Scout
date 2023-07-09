from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from tabulate import tabulate
from typing import Any, Dict, Tuple, List

import pandas as pd
import urlpath
import json
import datetime
import re
import time
import warnings
import threading
import pdb
import sys
import signal
import atexit

from tool import exception_info
from tool import MonitorData
from scout_warning import UndefinedOutputMode
from scout_warning import UndefinedGuardian


class Application:
    """This class defined Spark application information."""
    def __init__(self, 
            session_or_url	  : Any, 
            applications_api_link : str		= "api/v1/applications"
        ) -> None:

        """
        An application is defined by the Spark UI link and an application id
        :param session_or_url		: spark session or url of spark
        :param applications_api_link	: api link default is api/v1/applications
        """

        self.applications_api_link = applications_api_link

        if isinstance(session_or_url, SparkSession):
            """
            Get application web url from spark context.
            Get application id from spark context
            """
            sc = session_or_url.sparkContext
            self.web_url = sc.uiWebUrl
            self.application_id = sc.applicationId

        elif isinstance(session_or_url, str):
            """
            Get available application id
            """
            applications_df = self.get_application_ids(session_or_url)
            self.web_url = session_or_url
            self.application_id = applications_df["id"].iloc[0]
        else:
            raise TypeError(f"argument session_or_url is unsupported {type(session_or_url)}")
    def get_application_ids(self, web_url: str) -> pd.DataFrame:
        """Retrieve available application id."""
        try:
            url_path = urlpath.URL(
                web_url, self.applications_api_link
            )
            applications_df = pd.read_json(url_path)
            return applications_df
        except:
            exception_info()

class ExecutorComputer(Application):
    """This class compute application information."""
    def __init__(self, 
            session_or_url	 : Any, 
            applications_api_link: str		= "api/v1/applications",
            debug		 : bool		= True
        ) -> None:
        """
        Compute application information.
        :param session_or_url           : spark session or url of spark
        :param applications_api_link    : api link default is api/v1/applications
        :param debug			: open debug mode
        """

        super().__init__(session_or_url, applications_api_link)

        self.debug = debug

    def get_executors_info(self) -> pd.DataFrame:
        """ function to get executors information."""
        url_path = urlpath.URL(
            self.web_url,
            self.applications_api_link,
            self.application_id,
            "executors"
        )
        try:
            return  pd.read_json(url_path)
        except:
            exception_info()

    def log_executors_info(self) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        """Function to analysis executors information transfor to dict and add into database."""
        executors_df = self.get_executors_info()
        
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        executors_df['timestamp'] = executors_df.apply(lambda x: now, axis=1)
        executors_df['application_id'] = executors_df.apply(lambda x: self.application_id, axis=1)

        if self.debug:
            # debug mode, save raw data.
            raw_executors_df = executors_df
        
        try:
            executors_df = self.flatten_executors_info(executors_df)          # 展開 dataframe 中的資料結構
            executors_summary_dict = self.summary_executors(executors_df)     # 計算 executors 的使用率
            executors_summary_dict['timestamp'] = now                         # 填入時間
            executors_summary_dict['application_id'] = self.application_id    # 填入 application ID
            return raw_executors_df, executors_df, executors_summary_dict     # 回傳 原始資料, 各個 executor 資料, 計算後的結果
        except Exception as e:
            exception_info()

    @staticmethod
    def flatten_executors_info(executors_df: pd.DataFrame) -> pd.DataFrame:
        """
        flatten executors dataframe and prepare some metrices
        """
        
        if len(executors_df) == 0: return dict()

        memoryMetrics = executors_df["memoryMetrics"].dropna()
        memoryMetrics_df = pd.DataFrame.from_records(memoryMetrics, index=memoryMetrics.index)
        executors_df = executors_df.join(memoryMetrics_df)
        executors_df = executors_df.drop(columns=['memoryMetrics'])

        if "peakMemoryMetrics" in executors_df.columns:
            peakMemoryMetrics = executors_df["peakMemoryMetrics"].dropna()
            peakMemoryMetrics_df = pd.DataFrame.from_records(peakMemoryMetrics, index=peakMemoryMetrics.index)
            executors_df = executors_df.join(peakMemoryMetrics_df)
            executors_df = executors_df.drop(columns=['peakMemoryMetrics'])

        executors_df["usedOnHeapStorageMemoryPct"] = (
            executors_df["usedOnHeapStorageMemory"] / executors_df["totalOnHeapStorageMemory"] * 100
        )
        executors_df["usedOffHeapStorageMemoryPct"] = (
            executors_df["usedOffHeapStorageMemory"] / executors_df["totalOffHeapStorageMemory"] * 100
        )
        executors_df["memoryUsedPct"] = executors_df["memoryUsed"] / executors_df["maxMemory"] * 100
        executors_df = executors_df.drop(
            columns=[
                "executorLogs",
                "blacklistedInStages",
                "attributes",
                "resources",
                "excludedInStages"
            ]
        )
        return executors_df

    @staticmethod
    def summary_executors(executors_df: pd.DataFrame) -> Dict[str, Any]:
        """
        summary executors usage
        """
        # remove dirver info
        executors_df = executors_df.query("id!='driver'")
        agg_columns = [
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
        agg_columns = [col for col in agg_columns if col in executors_df.columns]
        
        # Obtain aggregated values
        # Let's filter: "RuntimeWarning: Mean of empty slice"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            res = executors_df[agg_columns].agg(["max", "mean", "median", "min"])
            """
            res:
                    memoryUsedPct  usedOnHeapStorageMemoryPct  usedOffHeapStorageMemoryPct  totalOnHeapStorageMemory  totalOffHeapStorageMemory
            max               0.0                         0.0                          NaN              2.101975e+09                        0.0
            mean              0.0                         0.0                          NaN              2.101975e+09                        0.0
            median            0.0                         0.0                          NaN              2.101975e+09                        0.0
            min               0.0                         0.0                          NaN              2.101975e+09                        0.0
            """
        # Convert to dict
        output: Dict[str, Any] = {
            f"{key}_{agg}": value for key, values in res.to_dict().items() for agg, value in values.items()
        }

        if len(res.dropna()) == 0:
            ana_cols = [
                'numActive', 'memoryUsed_sum', 'maxMemory_sum',
                'memoryUsed_sum_pct', 'memoryUsedPct_driver'
            ]
            ana = {col:None for col in ana_cols}
            return {**output, **ana}

        output['numActive'] = len(executors_df.query("isActive"))
        output["memoryUsed_sum"] = executors_df["memoryUsed"].sum()
        output["maxMemory_sum"] = executors_df["maxMemory"].sum()
        output["memoryUsed_sum_pct"] = executors_df["memoryUsed"].sum() / executors_df["maxMemory"].sum() * 100
        output["memoryUsedPct_driver"] = executors_df.iloc[0]["memoryUsedPct"]
        return output

class Scout(ExecutorComputer):
    """
    # Scout: spark monitor,
    ## monitor list:
    - executor memory
    - job stage 
    - job task
    ## output mode:
    - text file mode: write json like format data line by line to text file
    - kafka mode: output json like format to kafka (In progressing)
    """
    def __init__(self,
            session_or_url       : Any,
            configs		 : Dict[str, Any],
            applications_api_link: str          	= "api/v1/applications",
        ) -> None:

        self.configs = configs
        self.log_path = configs['log_path']            # path of log file
        self.debug = "debug" in self.configs.keys()    # 判斷是否需要debug mode
        self.monitors = self.registrate_monitor()      # 需求的監控
        self.time_interval = configs['time_interval']  # 監控的時間間隔

        ExecutorComputer.__init__(self, session_or_url, applications_api_link, self.debug)
        self.stop_flag = threading.Event()          # setup stop flag.

    def start_scout_daemon(self) -> None:
        """
        Use this function to start Scout daemon.
        """
        self.scout_daemon = threading.Thread(target=self.scout_driver)
        self.scout_daemon.daemin = True
        self.scout_daemon.start()

    def stop_scout_daemon(self) -> None:
        """
        Use this function to stop Scout daemon.
        """
        self.stop_flag.set()

    def join_scout_thread(self) -> None:
        """
        Use this function to join thread. 
        """
        self.scout_daemon.join()

    def scout_driver(self) -> None:
        """
        Scout 的 driver，透過這個function來成行監控
        """

        while not self.stop_flag.is_set():
            # 依序監控 monitors 的內容
            for func in self.monitors:
                func()
            time.sleep(15)

    def registrate_monitor(self) -> List:
        """
        註冊需要的監控並回傳，回傳的 List 中會包含所註冊的監控
        executor_monitor  : executor memory information
        job_stage_monitor : job stage information
        job_task_monitor  : job task information 
        """
        monitor = {
            "executor_monitor":self.executor_monitor,
            "job_stage_monitor":"",
            "job_task_monitor":""
        }

        output = list()
        for target in self.configs["guardian_type"]:
            try:
                output.append(monitor[target])
            except KeyError: 
                raise UndefinedGuardian(target)
            finally:
                exception_info()
        return output

    def executor_monitor(self) -> None:
        """
        監控產出
        - raw_executors_df       : application 的原始資料
        - executors_df           : executor 的資訊
        - executors_summary_dict : 統計後的 executor 資料
        """
        try:
            raw_executors_df, executors_df, executors_summary_dict = self.log_executors_info()

            action = self.get_output_mode() 

            action(executors_df, "info")
            action(executors_summary_dict, "summary")

            if self.debug: # if use debug mode, output all information
                action(raw_executors_df, "debug")
            
        except:
            exception_info()

    def write_text(self, data: Any, data_type: str) -> None:
        """
        This is output function, use this function to write text file.
        """
        file_name=f"{self.log_path}/{self.application_id}_{data_type}.txt"
        MonitorData(data).write2text(file_name=file_name)
    
    def kafka_launcher(self, data: Any, config: Dict[Any, Any]) -> None:
        """
        This is output function, use this function to send data to kafka by streaming
        """

    def get_output_mode(self) -> Any:
        """
        Defiend output mode and return output function
        """
        if self.configs["mode"] == "file":
            return self.write_text
        elif self.configs["mode"] == "kafka":
            return self.kafka_launcher
        else:
            raise UndefinedOutputMode(self.configs["mode"])
        
