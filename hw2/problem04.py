# From subtask (1), for the top-100 frequent words per topic in titles and headlines,
# calculate their co-occurrence matrices (100x100), respectively.
# Each entry in the matrix will contain the co-occurrence frequency in all news titles and headlines, respectively

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


def count_word(rdd, column_index):
    # 將每一列[IDLink, Title, Headline, ...]映射成單一列[Word, Word, Word, ...]
    word_rdd = rdd.flatMap(lambda row: row[column_index].split())
    # 將單一列[Word, Word, Word, ...]映射成[Word, 1]並以key(Word)歸納成[Word, n]
    word_rdd = word_rdd.map(lambda row: (row, 1))
    reduced_word_rdd = word_rdd.reduceByKey(lambda a, b: a + b)
    # 將每一列[Word, n]映射成[n, Word]並以key(count)排序(由大到小)
    reduced_word_rdd = reduced_word_rdd.map(lambda row: (row[1], row[0])).sortByKey(ascending = False)
    return reduced_word_rdd


def compute_co_occurrence(sentence_list, top100_list):
    co_occurrence_matrix = [[None] * 100 for i in range(100)]
    for i in range(100):
        for j in range(100):
            frequency = 0
            for row in sentence_list:
                if (top100_list[i] in row) and (top100_list[j] in row):
                    frequency = frequency + 1
            co_occurrence_matrix[i][j] = frequency
    return co_occurrence_matrix


def write_file_co_occurrence(co_occurrence_matrix_list):
    topics = ["economy", "microsoft", "obama", "palestine"]
    attributes = ["title", "headline"]
    output_file = open("task4_output/co-occurrence_matrix.txt", "w", encoding = "utf-8")
    for index in range(len(co_occurrence_matrix_list)):
        # 建立並寫入資料
        output_file.write("{} {} co-occurrence matrix\n".format(topics[index % 4], attributes[int(index / 4)]))
        for row in co_occurrence_matrix_list[index]:
            output_file.write("{}\n".format(row))
        output_file.write("\n")


if __name__ == "__main__":
    APP_NAME = "hw2_problem04"
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

    co_occurrence_matrix_list = []

    # 計算economy這個topic，title的co-occurrence矩陣
    economy_title_list = economy_rdd.map(lambda row: row[1].split()).collect()
    economy_title_top100_list = [row[1] for row in redueced_title_topic_rdds[0].take(100)]
    co_occurrence_matrix_list.append(compute_co_occurrence(economy_title_list, economy_title_top100_list))
    # 計算microsoft這個topic，title的co-occurrence矩陣
    microsoft_title_list = microsoft_rdd.map(lambda row: row[1].split()).collect()
    microsoft_title_top100_list = [row[1] for row in redueced_title_topic_rdds[1].take(100)]
    co_occurrence_matrix_list.append(compute_co_occurrence(microsoft_title_list, microsoft_title_top100_list))
    # 計算obama這個topic，title的co-occurrence矩陣
    obama_title_list = obama_rdd.map(lambda row: row[1].split()).collect()
    obama_title_top100_list = [row[1] for row in redueced_title_topic_rdds[2].take(100)]
    co_occurrence_matrix_list.append(compute_co_occurrence(obama_title_list, obama_title_top100_list))
    # 計算palestine這個topic，title的co-occurrence矩陣
    palestine_title_list = palestine_rdd.map(lambda row: row[1].split()).collect()
    palestine_title_top100_list = [row[1] for row in redueced_title_topic_rdds[3].take(100)]
    co_occurrence_matrix_list.append(compute_co_occurrence(palestine_title_list, palestine_title_top100_list))

    # 計算economy這個topic，headline的co-occurrence矩陣
    economy_headline_list = economy_rdd.map(lambda row: row[2].split()).collect()
    economy_headline_top100_list = [row[1] for row in redueced_headline_topic_rdds[0].take(100)]
    co_occurrence_matrix_list.append(compute_co_occurrence(economy_headline_list, economy_headline_top100_list))
    # 計算microsoft這個topic，headline的co-occurrence矩陣
    microsoft_headline_list = microsoft_rdd.map(lambda row: row[2].split()).collect()
    microsoft_headline_top100_list = [row[1] for row in redueced_headline_topic_rdds[1].take(100)]
    co_occurrence_matrix_list.append(compute_co_occurrence(microsoft_headline_list, microsoft_headline_top100_list))
    # 計算obama這個topic，headline的co-occurrence矩陣
    obama_headline_list = obama_rdd.map(lambda row: row[2].split()).collect()
    obama_headline_top100_list = [row[1] for row in redueced_headline_topic_rdds[2].take(100)]
    co_occurrence_matrix_list.append(compute_co_occurrence(obama_headline_list, obama_headline_top100_list))
    # 計算palestine這個topic，headline的co-occurrence矩陣
    palestine_headline_list = palestine_rdd.map(lambda row: row[2].split()).collect()
    palestine_headline_top100_list = [row[1] for row in redueced_headline_topic_rdds[3].take(100)]
    co_occurrence_matrix_list.append(compute_co_occurrence(palestine_headline_list, palestine_headline_top100_list))

    write_file_co_occurrence(co_occurrence_matrix_list)

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
