from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from tabulate import tabulate
from typing import Any, Dict, Tuple

import pandas as pd
import urlpath, json
import datetime, re, time
import warnings, threading

import pdb, sys

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
    # Scout: spark executor monitor,
    - type-I  : monitor spark executors memory information
    - type-II : monitor spark executors memory, task, stage (In progressing)
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
        self.debug = "debug" in self.configs.keys() #or self.configs["debug"]
        # Determine whether to use debug mode.

        ExecutorComputer.__init__(self, session_or_url, applications_api_link, self.debug)
        self.stop_flag = threading.Event() # setup stop flag.

    def start_scout_daemon(self):
        """
        Use this function to start Scout daemon.
        """
        self.scout_daemon = threading.Thread(target=self.engine)
        self.scout_daemon.daemin = True
        self.scout_daemon.start()

    def stop_scout_daemon(self):
        """
        Use this function to stop Scout daemon.
        """
        self.stop_flag.set()

    def join_scout_thread(self):
        """
        Use this function to join thread. 
        """
        self.scout_daemon.join()

    def engine(self):
        """
        Scout engine, use this function execute monitor.
        """

        guardian_type = self.configs["guardian_type"]
        print(f"Chose Guardian {guardian_type}, monitor spark executors, debug mode: {self.debug}")

        while not self.stop_flag.is_set():
            if self.configs["guardian_type"] == "type-I":
                self.type_I()
            else:
                raise UndefinedGuardian(self.configs['guardian_type'])
            time.sleep(15)

    def type_I(self):
        """
        monitor list:
        - spark application executor memory
        """
        try:
            raw_executors_df, executors_df, executors_summary_dict = self.log_executors_info()
            action = self.get_output_mode() # output mode: file, kafka
            action(executors_df, self.configs["output"]["executor_info"])
            action(executors_summary_dict, self.configs["output"]["summary"])
            if self.debug: # if use debug mode, output all information
                action(raw_executors_df, self.configs["debug"])
            
        except:
            exception_info()

    def write_text(self, data, configs):
        """
        This is output function, use this function to write text file.
        """
        file_name = configs["file_name"]
        mode = configs["mode"]
        MonitorData(data).write2text(file_name=file_name, mode=mode)
    
    def kafka_launcher(self, data, config):
        """
        This is output function, use this function to send data to kafka by streaming
        """

    def get_output_mode(self):
        """
        Defiend output mode and return output function
        """
        if self.configs["mode"] == "file":
            return self.write_text
        elif self.configs["mode"] == "kafka":
            return self.kafka_launcher
        else:
            raise UndefinedOutputMode(self.configs["mode"])
        

if __name__ == "__main__":
    configs = {
        "configs":{
            "spark.executor.memory":"1g",
            "spark.executor.cores":"1",
            "spark.driver.memory":"2g",
            "spark.driver.cores":"2",
            "spark.executor.instances":"1"
        },
        "appName": "test spark monitor",
        "master": "spark://spark-master:7077"
    }

    conf = SparkConf().setAll(configs['configs'].items())
    spark = SparkSession.builder\
                .appName(configs['appName'])\
                .master(configs['master'])\
                .config(conf=conf).getOrCreate()


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

    scout = Scout(spark.sparkContext.uiWebUrl, scout_configs)
    scout.start_scout_daemon()
    while True:
        time.sleep(5)
    scout.stop_scout_daemon()
    scout.join_scout_thread()

    spark.stop()
