import pandas as pd
from pyspark.sql import SparkSession
from flask import Flask, request, jsonify, render_template
from utils import calculate_cosine_similarity, generateQueryDocTf

def load_and_clean_movie_titles():
    # uploaded movie_metadata.tsv manually
    data = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load("corpus/movie.metadata.tsv")
    # rename the first and third column to wiki id and movie title
    data = data[["_c0", "_c2", "_c3", "_c5"]].withColumnRenamed("_c0", "WikipediaDocId").withColumnRenamed("_c2", "MovieTitle").withColumnRenamed("_c3", "ReleaseDate").withColumnRenamed("_c4", "Runtime").collect()

    return data

def get_formatted_result_df(result):
    # Read the data into a pandas df (since we only have 10 rows of data)
    df = pd.DataFrame(result)
    df.columns = ["WikipediaDocId", "Match"]

    # Add the MovieTitle, ReleaseDate, and Runtime to the df, where movie_metadata is a list of tuples
    df2 = pd.DataFrame(movie_metadata, columns = ["WikipediaDocId", "MovieTitle", "ReleaseDate", "Runtime"])
    df = df.merge(df2, on = "WikipediaDocId")

    return df[["WikipediaDocId", "MovieTitle", "Match", "ReleaseDate", "Runtime"]].sort_values("Match", ascending = False, ignore_index = True).head(20)

def handle_lists(query_list, doc_list):
    return [calculate_cosine_similarity(i, j) for i, j in zip(query_list, doc_list)]

def retrieve_movies(queries):
    query_rdd = sc.parallelize(queries)
    query_tf = query_rdd.map(generateQueryDocTf)

    multiple_word_query_tf = query_tf.map(lambda x: [(t[0], (x[1].count(t[0])/t[1])) for t in x[0]])
    multiple_word_query_tf.collect()

    # Generate the query tf-idf score
    multiple_query_tf_idf = multiple_word_query_tf.map(lambda x: [(t[0], t[1] * wordIdfScoreMap.get(t[0], 1)) for t in x])
    multiple_query_tf_idf_vector = multiple_query_tf_idf.map(lambda x: [t[1] for t in x]).collect()
    multiple_query_words = multiple_word_query_tf.map(lambda x: [t[0] for t in x]).collect()

    docid_vector = docid_list.map(lambda x : (x, [[corpus_tf_map.get((word, x), 0.0) * wordIdfScoreMap.get(word, 0.0) for word in query_word] for query_word in multiple_query_words]))

    cosine_sim_result = docid_vector.map(lambda x: (x[0], *handle_lists(multiple_query_tf_idf_vector, x[1]))).collect()
    # print(get_formatted_result_df(cosine_sim_result, multiple_word_query_tf))
    return get_formatted_result_df(cosine_sim_result).to_json(orient="records")

app = Flask(__name__, static_folder='build', static_url_path='/')
spark = SparkSession.builder.master("local").appName("SearchEngine").getOrCreate() # Get an existing SparkSession or create a new one
sc = spark.sparkContext

docid_list = sc.pickleFile("docid_list", 3)
corpus_tf_map = sc.pickleFile("corpus_tf").collectAsMap()
wordIdfScoreMap = sc.pickleFile("wordIdfScore").collectAsMap()
movie_metadata = load_and_clean_movie_titles()

@app.route('/', methods=['GET'])
def home():
    return app.send_static_file('index.html')

@app.route('/api/find_movies', methods=['POST'])
def find_movies():
    queries = request.json['queries']
    return retrieve_movies(queries)

if __name__ == '__main__':
    app.run(host='0.0.0.0')
