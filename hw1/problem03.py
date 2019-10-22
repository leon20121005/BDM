from pyspark import SparkConf, SparkContext
import time

def split_column(rdd):
    # 根據分號將每一列的每一行切開
    rdd = rdd.map(lambda line: line.split(";"))
    return rdd


def get_column(rdd, column_index):
    # 根據column_index取得指定欄位
    column = rdd.map(lambda row: row[column_index])
    # 移除該欄位中的?值，將字串型態的數字轉換成浮點數
    column = column.filter(lambda x: x != "?")
    column = column.map(lambda x: float(x))
    # 回傳該欄位
    return column


def get_column_min(column):
    # 計算最小值
    return column.min()


def get_column_max(column):
    # 計算最大值
    return column.max()


def perform_min_max_normalization(rdd, column_index, column_min, column_max):
    # min-max正規化
    difference = column_max - column_min
    rdd = rdd.map(lambda row: modify_value(row, column_index, column_min, difference))
    return rdd


def modify_value(row, column_index, column_min, difference):
    if row[column_index] != "?":
        row[column_index] = (float(row[column_index]) - column_min) / difference
    return row


if __name__ == "__main__":
    APP_NAME = "hw1_problem03"

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

    rdd = split_column(rdd)

    # 計算global_active_power的最小值和最大值
    global_active_power = get_column(rdd, 2)
    global_active_power_min = get_column_min(global_active_power)
    global_active_power_max = get_column_max(global_active_power)

    # 計算global_reactive_power的最小值和最大值
    global_reactive_power = get_column(rdd, 3)
    global_reactive_power_min = get_column_min(global_reactive_power)
    global_reactive_power_max = get_column_max(global_reactive_power)

    # 計算voltage的最小值和最大值
    voltage = get_column(rdd, 4)
    voltage_min = get_column_min(voltage)
    voltage_max = get_column_max(voltage)

    # 計算global_intensity的最小值和最大值
    global_intensity = get_column(rdd, 5)
    global_intensity_min = get_column_min(global_intensity)
    global_intensity_max = get_column_max(global_intensity)

    rdd = perform_min_max_normalization(rdd, 2, global_active_power_min, global_active_power_max)
    rdd = perform_min_max_normalization(rdd, 3, global_reactive_power_min, global_reactive_power_max)
    rdd = perform_min_max_normalization(rdd, 4, voltage_min, voltage_max)
    rdd = perform_min_max_normalization(rdd, 5, global_intensity_min, global_intensity_max)

    end_time = time.time() - start_time

    print("Start writing file...")

    output_file = open("output.txt", "w")
    output_file.write("{}\n".format(header))
    output_format = "{};{};{};{};{};{};{};{};{}\n"
    for row in rdd.collect():
        output_file.write(output_format.format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))

    print("Total running time (without writing file): {}".format(end_time))
    print("Total running time: {}".format(time.time() - start_time))
