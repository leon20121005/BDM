# Given the Reuters-21578 dataset, please calculate all k-shingles and output the set representation of the text dataset as a matrix.
# Set representation: A M x N matrix: with rows as shingles and columns as documents (N = 21,578)

from pyspark import SparkConf, SparkContext
import time
from bs4 import BeautifulSoup

def preprocess_data(rdd):
    rdd = rdd.flatMap(lambda pair: parse_document(pair))
    return rdd


def parse_document(pair):
    soup = BeautifulSoup(pair[1], "html.parser")
    body_list = soup.find_all("body")
    documents = [remove_noise(body.text) for body in body_list]
    return documents


def remove_noise(text):
    text = text.replace(".\n", " ")
    text = text.replace("\n", " ")
    text = text.replace(",", "")
    text = text.replace("Reuter", "")
    text = text.replace("\x03", "")
    return text


# 將[news1.text, news2.text, ...]映射成[[news1.shingle, news1.shingle, ...], [news2.shingle, news2.shingle, ...], ...]
def create_shingles(rdd):
    rdd = rdd.map(lambda document: create_shingle(document))
    return rdd


def create_shingle(document):
    document = document.split(" ")
    document = [word for word in document if word != ""]
    shingle = []
    for index in range(len(document) - 4):
        temp_row = []
        for k in range(SHINGLING_K):
            temp_row.append(document[index + k])
        shingle.append(temp_row)
    return shingle


# 將[[news1.shingle, news1.shingle, ...], [news2.shingle, news2.shingle, ...], ...]
# 映射成[shingle, shingle, shingle, shingle, ...]
# 映射成[[shingle, boolean, boolean, ...], [shingle, boolean, boolean, ...], ...]
def create_set_representation(news_shingles_rdd):
    all_shingles_rdd = remove_duplicate(news_shingles_rdd.flatMap(lambda shingle: shingle))
    news_shingles = news_shingles_rdd.collect()
    set_representation_rdd = all_shingles_rdd.map(lambda shingle: create_set_row(shingle, news_shingles))
    return set_representation_rdd


def remove_duplicate(rdd):
    rdd = rdd.map(lambda shingle: combine_words(shingle))
    rdd = rdd.distinct()
    rdd = rdd.map(lambda shingle_string: shingle_string.split(" "))
    return rdd


def combine_words(shingle):
    shingle_string = ""
    for word in shingle:
        shingle_string += (word + " ")
    shingle_string = shingle_string[:-1]
    return shingle_string


# 對單一shingle，遍歷每篇新聞並判斷目標shingle是否出現在該新聞中(以shingle為列)
def create_set_row(shingle, news_shingles):
    temp_row = []
    temp_row.append(shingle)
    for each_news_shingles in news_shingles:
        if shingle in each_news_shingles:
            temp_row.append(1)
        else:
            temp_row.append(0)
    return temp_row


if __name__ == "__main__":
    APP_NAME = "hw3_problem01"
    MASTER_URL = "local[*]"
    HOME_PATH = ".\\data\\"
    SHINGLING_K = 2

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    # 讀取檔案並進行預處理以取得每篇新聞的body.text
    news_rdd = preprocess_data(sc.wholeTextFiles(HOME_PATH + "reut2-001.sgm" + "," + HOME_PATH + "reut2-002.sgm"))

    # 計算每篇新聞的shingles
    news_shingles_rdd = create_shingles(news_rdd)

    set_representation_rdd = create_set_representation(news_shingles_rdd)
    set_representation_rdd.saveAsTextFile("output01_{}".format(time.strftime("%Y%m%d%H%M", time.localtime())))

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
