# Given the term-document matrix in (1), compute the SVD decomposition of the matrix using MapReduce. Output the resulting eigenvalues and eigenvectors.
# eigenpairs:
# Eigenvalues sorted in descending order, and their corresponding eigenvectors

from pyspark import SparkConf, SparkContext
import time
from bs4 import BeautifulSoup
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import RowMatrix
import numpy as np

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


def create_terms(rdd):
    rdd = rdd.zipWithIndex()
    rdd = rdd.map(lambda pair: create_term(pair[0], pair[1]))
    return rdd


def create_term(document, news_index):
    document = document.split(" ")
    terms = [((term, news_index), 1) for term in document if term != ""]
    return terms


def group_terms(rdd):
    rdd = rdd.flatMap(lambda terms: terms)
    rdd = rdd.reduceByKey(lambda a, b: a + b)
    rdd = rdd.map(lambda term: (term[0][0], (term[0][1], term[1])))
    rdd = rdd.reduceByKey(lambda a, b: combine(a, b))
    return rdd


def combine(a, b):
    if type(a[0]) is int and type(b[0]) is int:
        return [a, b]
    if type(a[0]) is not int and type(b[0]) is int:
        a.append(b)
        return a
    if type(a[0]) is int and type(b[0]) is not int:
        b.append(a)
        return b
    if type(a[0]) is not int and type(b[0]) is not int:
        return a + b


def create_sparse_matrix(rdd):
    rdd = rdd.map(lambda row: create_sparse_vector(row))
    matrix = RowMatrix(rdd)
    return matrix


def create_sparse_vector(row):
    term = row[0]
    if type(row[1][0]) is int:
        dictionary = {}
        dictionary[row[1][0]] = row[1][1]
    else:
        documents = row[1]
        dictionary = {}
        for document in documents:
            dictionary[document[0]] = document[1]
    return Vectors.sparse(N, dictionary)


if __name__ == "__main__":
    APP_NAME = "hw4_problem03"
    MASTER_URL = "spark://192.168.1.103:7077"
    HOME_PATH = "D:\\data\\"

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    # 讀取檔案並進行預處理以取得每篇新聞的body.text
    document_rdd = preprocess_data(sc.wholeTextFiles(HOME_PATH + "reut2-[0-9]*.sgm"))
    # 計算每篇新聞的terms
    document_terms_rdd = create_terms(document_rdd)
    # 計算每個terms的新聞
    terms_document_rdd = group_terms(document_terms_rdd)

    N = document_terms_rdd.count()

    matrix = create_sparse_matrix(terms_document_rdd)

    # Compute the top 5 singular values and corresponding singular vectors.
    svd = matrix.computeSVD(5, computeU = False)
    U = svd.U       # The U factor is a RowMatrix.
    s = svd.s       # The singular values are stored in a local dense vector.
    V = svd.V       # The V factor is a local dense matrix.

    # U_rdd = U.rows.map(lambda row: row.toArray().tolist())
    s = s.toArray().tolist()
    V = V.toArray().tolist()

    # s_matrix = np.diag(s)
    # matrix = np.dot(np.dot(U_rdd.collect(), s), np.transpose(V))

    V_rdd = sc.parallelize(np.transpose(V).tolist()).coalesce(1)
    s_rdd = sc.parallelize(s).coalesce(1)
    s_rdd = s_rdd.union(V_rdd)
    s_rdd.saveAsTextFile("D:\\output03_{}".format(time.strftime("%Y%m%d%H%M", time.localtime())))

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
