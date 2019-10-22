from pyspark import SparkConf, SparkContext
import time

def get_column(rdd, column_index):
    # 根據分號將每一列的每一行切開，根據column_index取得指定欄位
    column = rdd.map(lambda line: line.split(";")[column_index])
    # 移除該欄位中的?值，將字串型態的數字轉換成浮點數
    column = column.filter(lambda x: x != "?")
    column = column.map(lambda x: float(x))
    # 回傳該欄位
    return column


def get_column_mean(column):
    # 計算平均值
    return column.mean()


def get_column_standard_deviation(column):
    # 計算標準差
    return column.stdev()


if __name__ == "__main__":
    APP_NAME = "hw1_problem02"

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

    # 計算global_active_power的平均值、標準差
    global_active_power = get_column(rdd, 2)
    global_active_power_mean = get_column_mean(global_active_power)
    global_active_power_standard_deviation = get_column_standard_deviation(global_active_power)

    # 計算global_reactive_power的平均值、標準差
    global_reactive_power = get_column(rdd, 3)
    global_reactive_power_mean = get_column_mean(global_reactive_power)
    global_reactive_power_standard_deviation = get_column_standard_deviation(global_reactive_power)

    # 計算voltage的平均值、標準差
    voltage = get_column(rdd, 4)
    voltage_mean = get_column_mean(voltage)
    voltage_standard_deviation = get_column_standard_deviation(voltage)

    # 計算global_intensity的平均值、標準差
    global_intensity = get_column(rdd, 5)
    global_intensity_mean = get_column_mean(global_intensity)
    global_intensity_standard_deviation = get_column_standard_deviation(global_intensity)

    print("Mean of global active power: {}".format(global_active_power_mean))
    print("Mean of global reactive power: {}".format(global_reactive_power_mean))
    print("Mean of voltage: {}".format(voltage_mean))
    print("Mean of global intensity: {}".format(global_intensity_mean))

    print("Standard deviation of global active power: {}".format(global_active_power_standard_deviation))
    print("Standard deviation of global reactive power: {}".format(global_reactive_power_standard_deviation))
    print("Standard deviation of voltage: {}".format(voltage_standard_deviation))
    print("Standard deviation of global intensity: {}".format(global_intensity_standard_deviation))

    print("Total running time: {}".format(time.time() - start_time))
