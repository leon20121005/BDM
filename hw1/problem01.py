from pyspark import SparkConf, SparkContext
import time

def get_column(rdd, column_index):
    # 根據分號將每一列的每一行切開，根據column_index取得指定欄位
    column = rdd.map(lambda line: line.split(";")[column_index])
    # 移除該欄位中的?值
    column = column.filter(lambda x: x != "?")
    # 回傳該欄位
    return column  


def get_column_min(column):
    # 計算最小值
    return column.min()


def get_column_max(column):
    # 計算最大值
    return column.max()


def get_column_count(column):
    # 計算總列數
    return column.count()


if __name__ == "__main__":
    APP_NAME = "hw1_problem01"

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("spark://10.100.10.211:7077")
    sc = SparkContext(conf = conf)

    # 讀取檔案
    rdd = sc.textFile("D:\household_power_consumption.txt")

    # 移除header
    header = rdd.first()
    rdd = rdd.filter(lambda line: line != header)

    # 計算global_active_power的最小值、最大值和總列數
    global_active_power = get_column(rdd, 2)
    global_active_power_min = get_column_min(global_active_power)
    global_active_power_max = get_column_max(global_active_power)
    global_active_power_count = get_column_count(global_active_power)

    # 計算global_reactive_power的最小值、最大值和總列數
    global_reactive_power = get_column(rdd, 3)
    global_reactive_power_min = get_column_min(global_reactive_power)
    global_reactive_power_max = get_column_max(global_reactive_power)
    global_reactive_power_count = get_column_count(global_reactive_power)

    # 計算voltage的最小值、最大值和總列數
    voltage = get_column(rdd, 4)
    voltage_min = get_column_min(voltage)
    voltage_max = get_column_max(voltage)
    voltage_count = get_column_count(voltage)

    # 計算global_intensity的最小值、最大值和總列數
    global_intensity = get_column(rdd, 5)
    global_intensity_min = get_column_min(global_intensity)
    global_intensity_max = get_column_max(global_intensity)
    global_intensity_count = get_column_count(global_intensity)

    print("Minimum of global active power: {}".format(global_active_power_min))
    print("Minimum of global reactive power: {}".format(global_reactive_power_min))
    print("Minimum of voltage: {}".format(voltage_min))
    print("Minimum of global intensity: {}".format(global_intensity_min))

    print("Maximum of global active power: {}".format(global_active_power_max))
    print("Maximum of global reactive power: {}".format(global_reactive_power_max))
    print("Maximum of voltage: {}".format(voltage_max))
    print("Maximum of global intensity: {}".format(global_intensity_max))

    print("Row count of global active power: {}".format(global_active_power_count))
    print("Row count of global reactive power: {}".format(global_reactive_power_count))
    print("Row count of voltage: {}".format(voltage_count))
    print("Row count of global intensity: {}".format(global_intensity_count))

    print("Total running time: {}".format(time.time() - start_time)) 
