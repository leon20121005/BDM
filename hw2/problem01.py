# In news data, count the words in two fields: ‘Title’ and ‘Headline’ respectively,
# and list the most frequent words according to the term frequency in descending order, in total, per day, and per topic, respectively

from pyspark import SparkConf, SparkContext
import time
from datetime import datetime

# 移除header並根據逗號將每一列的每一行切開
def preprocess_data(rdd):
    header = rdd.first()
    rdd = rdd.filter(lambda line: line != header)
    rdd = rdd.map(lambda row: split_values(row))
    return rdd


def split_values(row):
    # 將'IDLink,"Title","Headline","Source","Topic","PublishDate",SentimentTitle,SentimentHeadline,Facebook,GooglePlus,LinkedIn'
    # 根據,"切成['IDLink', 'Title"', 'Headline"', 'Source"', 'Topic"', 'PublishDate",SentimentTitle,SentimentHeadline,Facebook,GooglePlus,LinkedIn']
    row = row.split(',"')
    # 移除第2到第5個元素最後面的"，讓row變成：
    # ['IDLink', 'Title', 'Headline', 'Source', 'Topic', 'PublishDate",SentimentTitle,SentimentHeadline,Facebook,GooglePlus,LinkedIn']
    for index in range(1, len(row) - 1):
        row[index] = row[index][:-1]
    # 提取row除了最後一個元素的部分(row[:-1])
    new_row = row[:-1]
    # 將row最後一個元素(row[-1])根據",切成['PublishDate', 'SentimentTitle,SentimentHeadline,Facebook,GooglePlus,LinkedIn']並取得左半部分的'PublishDate'
    date = row[-1].split('",')[0]
    # 將右半部分根據,切成['SentimentTitle', 'SentimentHeadline', 'Facebook', 'GooglePlus', 'LinkedIn']
    ohters = row[-1].split('",')[1].split(',')
    # 將上述結果new_row、date、others合併起來完成切割
    new_row.append(date)
    new_row = new_row + ohters
    return new_row


def split_values_another(row):
    # 將'IDLink,"Title","Headline","Source","Topic","PublishDate",SentimentTitle,SentimentHeadline,Facebook,GooglePlus,LinkedIn'
    # 根據",切成['IDLink,"Title', '"Headline', '"Source', '"Topic', '"PublishDate', 'SentimentTitle,SentimentHeadline,Facebook,GooglePlus,LinkedIn']
    row = row.split('",')
    # 移除第2到第5個元素最前面的"，讓row變成：
    # ['IDLink,"Title', 'Headline', 'Source', 'Topic', 'PublishDate', 'SentimentTitle,SentimentHeadline,Facebook,GooglePlus,LinkedIn']
    for index in range(1, len(row) - 1):
        row[index] = row[index][1:]
    # 提取row除了第一個和最後一個元素的部分(row[1:-1])
    new_row = row[1:-1]
    # 將row第一個元素根據,"切成['IDLink', 'Title']
    id_link = row[0].split(',"')[0]
    title = row[0].split(',"')[1]
    # 將row最後一個元素根據,切成['SentimentTitle', 'SentimentHeadline', 'Facebook', 'GooglePlus', 'LinkedIn']
    others = row[-1].split(',')
    # 將上述結果id_link、title、new_row、others合併起來完成切割
    new_row = [id_link, title] + new_row + others
    return new_row


def count_word(rdd, column_index):
    # 將每一列[IDLink, Title, Headline, ...]映射成單一列[Word, Word, Word, ...]
    word_rdd = rdd.flatMap(lambda row: row[column_index].split())
    # 將單一列[Word, Word, Word, ...]映射成[Word, 1]並以key(Word)歸納成[Word, n]
    word_rdd = word_rdd.map(lambda row: (row, 1))
    reduced_word_rdd = word_rdd.reduceByKey(lambda a, b: a + b)
    # 將每一列[Word, n]映射成[n, Word]並以key(count)排序(由大到小)
    reduced_word_rdd = reduced_word_rdd.map(lambda row: (row[1], row[0])).sortByKey(ascending = False)
    return reduced_word_rdd


def count_word_per_day(rdd, column_index):
    # 將每一列[IDLink, Title, Headline, Source, Topic, PublishDate,...]
    # 映射成單一列[[[PublishDate, Word], 1], [[PublishDate, Word], 1], ...]並以key(PublishDate, Word)歸納成[[PublishDate, Word], n]
    word_rdd = rdd.flatMap(lambda row: pack_row(row, column_index))
    reduced_word_rdd = word_rdd.reduceByKey(lambda a, b: a + b)
    # 將每一列[[PublishDate, Word], n]映射成[[PublishDate, n], Word]並以key(PublishDate, count)排序(由新到舊，由大到小)
    reduced_word_rdd = reduced_word_rdd.map(lambda x: ((x[0][0], x[1]), x[0][1])).sortByKey(ascending = False)
    return reduced_word_rdd


def pack_row(row, column_index):
    date = parse_datetime(row[5])
    words = row[column_index].split()
    key_value_pair = []
    for word in words:
        key_value_pair.append(((date, word), 1))
    return key_value_pair


def parse_datetime(string):
    # 因為切割資料發生例外而讀不到正確日期而使用的自訂日期
    EXCEPTION_DATE = "2000-01-01"
    try:
        # 提取PublishDate日期的部份
        date = string.split(" ")[0]
        return datetime.strptime(date, "%Y-%m-%d")
    except ValueError as error:
        return datetime.strptime(EXCEPTION_DATE, "%Y-%m-%d")


def write_file_in_total(rdd, column_index):
    file_names = ["titles", "headlines"]
    output_file_format = "task1_output/top_frequent_{}_in_total.csv"
    output_file = open(output_file_format.format(file_names[column_index - 1]), "w", encoding = "utf-8")
    # 建立並寫入header
    header = '"Word","Count"\n'
    output_file.write(header)
    # 建立並寫入資料
    output_format = '"{}",{}\n'
    for row in rdd.collect():
        output_file.write(output_format.format(row[1], row[0]))


def write_file_per_topic(rdds, column_index):
    file_names = ["titles", "headlines"]
    topics = ["economy", "microsoft", "obama", "palestine"]
    output_file_format = "task1_output/top_frequent_{}_in_{}.csv"
    for index in range(4):
        output_file = open(output_file_format.format(file_names[column_index - 1], topics[index]), "w", encoding = "utf-8")
        # 建立並寫入header
        header = '"Word","Count"\n'
        output_file.write(header)
        # 建立並寫入資料
        output_format = '"{}",{}\n'
        for row in rdds[index].collect():
            output_file.write(output_format.format(row[1], row[0]))


def write_file_per_day(rdd, column_index):
    file_names = ["titles", "headlines"]
    output_file_format = "task1_output/top_frequent_{}_per_day.csv"
    output_file = open(output_file_format.format(file_names[column_index - 1]), "w", encoding = "utf-8")
    # 建立並寫入header
    header = '"PublishDate","Word","Count"\n'
    output_file.write(header)
    # 建立並寫入資料
    output_format = '"{}","{}",{}\n'
    for row in rdd.collect():
        output_file.write(output_format.format(row[0][0].date(), row[1], row[0][1]))


if __name__ == "__main__":
    APP_NAME = "hw2_problem01"
    MASTER_URL = "spark://192.168.1.102:7077"
    FILE_NAME = "D:\\data\\News_Final.csv"

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    # 讀取檔案並進行預處理
    rdd = preprocess_data(sc.textFile(FILE_NAME))

    # 對於所有topic，分別計算title和headline中每個word的數量
    reduced_title_rdd = count_word(rdd, 1)
    reduced_headline_rdd = count_word(rdd, 2)

    redueced_title_topic_rdds = []
    redueced_headline_topic_rdds = []

    # 對於economy這個topic，分別計算title和headline中每個word的數量並存入list
    economy_rdd = rdd.filter(lambda row: row[4] == "economy")
    redueced_title_topic_rdds.append(count_word(economy_rdd, 1))
    redueced_headline_topic_rdds.append(count_word(economy_rdd, 2))

    # 對於microsoft這個topic，分別計算title和headline中每個word的數量並存入list
    microsoft_rdd = rdd.filter(lambda row: row[4] == "microsoft")
    redueced_title_topic_rdds.append(count_word(microsoft_rdd, 1))
    redueced_headline_topic_rdds.append(count_word(microsoft_rdd, 2))

    # 對於obama這個topic，分別計算title和headline中每個word的數量並存入list
    obama_rdd = rdd.filter(lambda row: row[4] == "obama")
    redueced_title_topic_rdds.append(count_word(obama_rdd, 1))
    redueced_headline_topic_rdds.append(count_word(obama_rdd, 2))

    # 對於palestine這個topic，分別計算title和headline中每個word的數量並存入list
    palestine_rdd = rdd.filter(lambda row: row[4] == "palestine")
    redueced_title_topic_rdds.append(count_word(palestine_rdd, 1))
    redueced_headline_topic_rdds.append(count_word(palestine_rdd, 2))

    write_file_in_total(reduced_title_rdd, 1)
    write_file_in_total(reduced_headline_rdd, 2)
    write_file_per_topic(redueced_title_topic_rdds, 1)
    write_file_per_topic(redueced_headline_topic_rdds, 2)

    # 對於所有topic，計算每天title和headline中每個word的數量
    write_file_per_day(count_word_per_day(rdd, 1), 1)
    write_file_per_day(count_word_per_day(rdd, 2), 2)

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
