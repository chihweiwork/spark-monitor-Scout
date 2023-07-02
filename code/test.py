from pyspark.sql import SparkSession
import pandas as pd
import pdb, json
import requests

# 建立SparkSession物件
spark = SparkSession.builder \
    .appName("Spark 監控") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()



df = pd.DataFrame({
    "A":[x for x in range(0, 2000)],
    "B":[x for x in range(2000, 4000)],
    "c":[x for x in range(4000, 6000)]
})

sdf = spark.createDataFrame(df)

sc = spark.sparkContext


# 獲取 Spark UI 的 URL
spark_ui_url = spark.sparkContext.uiWebUrl

# 發送 HTTP 請求以獲取 Spark UI 的 JSON 數據
response = requests.get(spark_ui_url + "/api/v1/applications/" + spark.sparkContext.applicationId + "/allexecutors")

# 解析 JSON 數據
executor_info = response.json()

# 輸出記憶體和 CPU 資訊
for executor in executor_info:
    executor_id = executor["id"]
    memory_used = executor["memoryUsed"]
    cpu_used = executor["totalCores"]
    
    print(f"Executor ID: {executor_id}")
    print(f"Memory used: {memory_used} bytes")
    print(f"CPU usage: {cpu_used} cores")
    print("--------------")

pdb.set_trace()

spark.stop()


#memory_status = sc.getExecutorMemoryStatus()
#for executor, memory in memory_status.items():
#    print(f"執行器 {executor} 的記憶體使用情況: {memory}")
#
#cpu_status = sc.getExecutorCPUStatus()
#
#for executor, cpu in cpu_status.items():
#    print(f"執行器 {executor} 的CPU使用情況: {cpu}")

# 獲取 StatusTracker 物件
#status_tracker = sc.statusTracker()
#print(json.dumps(dir(status_tracker), indent=4))
#pdb.set_trace()

# 獲取執行器資訊
#executor_infos = status_tracker.getExecutorInfos()

# 輸出執行器記憶體和 CPU 使用情況
#for executor_info in sc._jsc.getExecutorMemoryStatus():
#for executor_info in sc.getExecutorMemoryStatus():
#    executor_id = executor_info.executorId()
#    memory_used = executor_info.memoryUsed()
#    cpu_utilization = executor_info.totalCores() - executor_info.totalCoresRemaining()
#    print(f"執行器 {executor_id} 的記憶體使用情況: {memory_used}")
#    print(f"執行器 {executor_id} 的 CPU 使用情況: {cpu_utilization}")


spark.stop()
