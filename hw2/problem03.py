from pyspark import SparkConf, SparkContext, sql
import time
from datetime import datetime

if __name__ == "__main__":
    APP_NAME = "hw2_problem03"
    MASTER_URL = "spark://192.168.1.102:7077"
    FILE_NAME = "D:\\data\\News_Final.csv"

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    spark = sql.SparkSession.builder.master(MASTER_URL).appName(APP_NAME).getOrCreate()

    # 讀檔
    data = spark.read.format("csv").option("header", "True").option("escape", '"').load(FILE_NAME)

    # 依照Topic取出該項目的SentimentTitle, SentimentHeadline並相加
    obama_ST = data.filter("Topic = 'obama'").select("SentimentTitle", "SentimentHeadline").rdd.map(lambda x: float(x[0]) + float(x[1]))
    economy_ST = data.filter("Topic = 'economy'").select("SentimentTitle", "SentimentHeadline").rdd.map(lambda x: float(x[0]) + float(x[1]))
    microsoft_ST = data.filter("Topic = 'microsoft'").select("SentimentTitle", "SentimentHeadline").rdd.map(lambda x: float(x[0]) + float(x[1]))
    palestine_ST = data.filter("Topic = 'palestine'").select("SentimentTitle", "SentimentHeadline").rdd.map(lambda x: float(x[0]) + float(x[1]))

    # 總和
    obama_ST_sum = obama_ST.sum()
    economy_ST_sum = economy_ST.sum()
    microsoft_ST_sum = microsoft_ST.sum()
    palestine_ST_sum = palestine_ST.sum()

    # 計算平均
    obama_ST_avg = obama_ST_sum / obama_ST.count()
    economy_ST_avg = economy_ST_sum / economy_ST.count()
    microsoft_ST_avg = microsoft_ST_sum / microsoft_ST.count()
    palestine_ST_avg = palestine_ST_sum / palestine_ST.count()

    print("obama sentiment score sum: {}".format(obama_ST_sum))
    print("obama sentiment score average: {}".format(obama_ST_avg))
    print("economy sentiment score sum: {}".format(economy_ST_sum))
    print("economy sentiment score average: {}".format(economy_ST_avg))
    print("microsoft sentiment score sum: {}".format(microsoft_ST_sum))
    print("microsoft sentiment score average: {}".format(microsoft_ST_avg))
    print("palestine sentiment score sum: {}".format(palestine_ST_sum))
    print("palestine sentiment score average: {}".format(palestine_ST_avg))

    print("Total running time: {}".format(time.time() - start_time))
