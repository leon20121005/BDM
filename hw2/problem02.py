# In social feedback data, calculate the average popularity of each news by hour, and by day, respectively (for each platform)

from pyspark import SparkConf, SparkContext
import time

def union_facebook(sc, file_names):
    # 讀取檔案並進行預處理
    rdd_facebook_economy = preprocess_data(sc.textFile(file_names[0]))
    rdd_facebook_microsoft = preprocess_data(sc.textFile(file_names[1]))
    rdd_facebook_obama = preprocess_data(sc.textFile(file_names[2]))
    rdd_facebook_palestine = preprocess_data(sc.textFile(file_names[3]))
    # 合併同一個平台的資料並根據IDLink排序
    rdd_facebook = rdd_facebook_economy.union(rdd_facebook_microsoft)
    rdd_facebook = rdd_facebook.union(rdd_facebook_obama).union(rdd_facebook_palestine)
    return rdd_facebook


def union_google(sc, file_names):
    # 讀取檔案並進行預處理
    rdd_google_economy = preprocess_data(sc.textFile(file_names[0]))
    rdd_google_microsoft = preprocess_data(sc.textFile(file_names[1]))
    rdd_google_obama = preprocess_data(sc.textFile(file_names[2]))
    rdd_google_palestine = preprocess_data(sc.textFile(file_names[3]))
    # 合併同一個平台的資料並根據IDLink排序
    rdd_google = rdd_google_economy.union(rdd_google_microsoft)
    rdd_google = rdd_google.union(rdd_google_obama).union(rdd_google_palestine)
    return rdd_google


def union_linkedin(sc, file_names):
    # 讀取檔案並進行預處理
    rdd_linkedin_economy = preprocess_data(sc.textFile(file_names[0]))
    rdd_linkedin_microsoft = preprocess_data(sc.textFile(file_names[1]))
    rdd_linkedin_obama = preprocess_data(sc.textFile(file_names[2]))
    rdd_linkedin_palestine = preprocess_data(sc.textFile(file_names[3]))
    # 合併同一個平台的資料並根據IDLink排序
    rdd_linkedin = rdd_linkedin_economy.union(rdd_linkedin_microsoft)
    rdd_linkedin = rdd_linkedin.union(rdd_linkedin_obama).union(rdd_linkedin_palestine)
    return rdd_linkedin


# 移除header並根據逗號將每一列的每一行切開
def preprocess_data(rdd):
    header = rdd.first()
    rdd = rdd.filter(lambda line: line != header)
    rdd = rdd.map(lambda row: row.split(","))
    return rdd


# 將row[IDLink, TS1, TS2, TS3, ..., TS144]合併成row[IDLink, 平均每小時(TS1, TS2, ..., TS144)]
def calculate_average_by_hour(row):
    row_per_hour = []
    row_per_hour.append(row[0])

    sum = 0
    for index in range(1, len(row)):
        sum += float(row[index])
    # 總共48小時
    average = sum / 48
    row_per_hour.append(average)
    return row_per_hour


# 將row[IDLink, TS1, TS2, TS3, ..., TS144]合併成row[IDLink, 平均每天(TS1, TS2, ..., TS144)]
def calculate_average_by_day(row):
    row_per_day = []
    row_per_day.append(row[0])

    sum = 0
    for index in range(1, len(row)):
        sum += float(row[index])
    # 總共2天
    average = sum / 2
    row_per_day.append(average)
    return row_per_day


def write_file_by_hour(file_name, rdd):
    output_file = open(file_name, "w")

    # 建立並寫入header
    header = '"IDLink","Average by hour"\n'
    output_file.write(header)

    # 建立並寫入資料
    output_format = "{},{}\n"
    for row in rdd.collect():
        output_file.write(output_format.format(row[0], row[1]))


def write_file_by_day(file_name, rdd):
    output_file = open(file_name, "w")

    # 建立並寫入header
    header = '"IDLink","Average by day"\n'
    output_file.write(header)

    # 建立並寫入資料
    output_format = "{},{}\n"
    for row in rdd.collect():
        output_file.write(output_format.format(row[0], row[1]))


if __name__ == "__main__":
    APP_NAME = "hw2_problem02"
    MASTER_URL = "spark://192.168.1.102:7077"
    home_path = "D:\\data\\"
    facebook_file_names = ["Facebook_Economy.csv", "Facebook_Microsoft.csv", "Facebook_Obama.csv", "Facebook_Palestine.csv"]
    facebook_file_names = [home_path + file_name for file_name in facebook_file_names]
    google_file_names = ["GooglePlus_Economy.csv", "GooglePlus_Microsoft.csv", "GooglePlus_Obama.csv", "GooglePlus_Palestine.csv"]
    google_file_names = [home_path + file_name for file_name in google_file_names]
    linkedin_file_names = ["LinkedIn_Economy.csv", "LinkedIn_Microsoft.csv", "LinkedIn_Obama.csv", "LinkedIn_Palestine.csv"]
    linkedin_file_names = [home_path + file_name for file_name in linkedin_file_names]

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    rdd_facebook = union_facebook(sc, facebook_file_names)
    # 建立每小時和每天的平均資料並輸出檔案
    rdd_facebook_by_hour = rdd_facebook.map(lambda row: calculate_average_by_hour(row))
    rdd_facebook_by_day = rdd_facebook.map(lambda row: calculate_average_by_day(row))
    write_file_by_hour("task2_output/facebook_average_by_hour.csv", rdd_facebook_by_hour)
    write_file_by_day("task2_output/facebook_average_by_day.csv", rdd_facebook_by_day)

    rdd_google = union_google(sc, google_file_names)
    # 建立每小時和每天的平均資料並輸出檔案
    rdd_google_by_hour = rdd_google.map(lambda row: calculate_average_by_hour(row))
    rdd_google_by_day = rdd_google.map(lambda row: calculate_average_by_day(row))
    write_file_by_hour("task2_output/google_average_by_hour.csv", rdd_google_by_hour)
    write_file_by_day("task2_output/google_average_by_day.csv", rdd_google_by_day)

    rdd_linkedin = union_linkedin(sc, linkedin_file_names)
    # 建立每小時和每天的平均資料並輸出檔案
    rdd_linkedin_by_hour = rdd_linkedin.map(lambda row: calculate_average_by_hour(row))
    rdd_linkedin_by_day = rdd_linkedin.map(lambda row: calculate_average_by_day(row))
    write_file_by_hour("task2_output/linkedin_average_by_hour.csv", rdd_linkedin_by_hour)
    write_file_by_day("task2_output/linkedin_average_by_day.csv", rdd_linkedin_by_day)

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
