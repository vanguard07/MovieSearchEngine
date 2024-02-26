import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
import nltk
import string
from nltk.corpus import stopwords
from flask import Flask, request, jsonify, render_template

nltk.download('stopwords')
english_stopwords = set(stopwords.words('english'))

def sanitise_summary(summary):
    punctuation_removed_summary = summary
    for char in string.punctuation:
        punctuation_removed_summary = punctuation_removed_summary.replace(char, ' ')
    lowercase_punctuation_removed_summary = punctuation_removed_summary.lower().strip()
    return lowercase_punctuation_removed_summary

def remove_stopwords(summary):
    sanitised_summary = sanitise_summary(summary)
    tokens = sanitised_summary.split()
    cleaned_words = [word for word in tokens if word not in english_stopwords]
    return cleaned_words

def generateQueryDocTf(summary):
    clean_tokens = remove_stopwords(summary)
    query_docLen_tuple_list = [(word, len(clean_tokens)) for word in clean_tokens]
    return list(set(query_docLen_tuple_list)), summary.lower()

def calculate_cosine_similarity(query, doc):
    query_magnitude = np.sqrt(np.sum([i ** 2 for i in query]))
    doc_magnitude = np.sqrt(np.sum([i ** 2 for i in doc]))
    product = np.sum([i * j for i, j in zip(query, doc)])
    if query_magnitude == 0 or doc_magnitude == 0:
        return 0
    return product / (query_magnitude * doc_magnitude)

def load_and_clean_movie_titles():
    # uploaded movie_metadata.tsv manually
    data = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load("jobs/movie_metadata.tsv")
    # rename the first and third column to wiki id and movie title
    data = data[["_c0", "_c2"]].withColumnRenamed("_c0", "WikipediaDocId").withColumnRenamed("_c2", "MovieTitle").collect()
    # create a dict from the table with key as doc_id and value as movie title
    data = {row_item.WikipediaDocId: row_item.MovieTitle for row_item in data}
    
    return data

def get_formatted_result_df(result, word_query_tf):
    # Read the data into a pandas df (since we only have 10 rows of data)
    df = pd.DataFrame(result)
    df.columns = ["WikipediaDocId", "Match"]

    # Add the MovieTitle column based on the wikipedia id
    df["MovieTitle"] = df.apply(lambda row: movie_metadata.get(row.WikipediaDocId, "!!Error!!Title_Unknown!!"), axis = 1)

    return df[["WikipediaDocId", "MovieTitle", "Match"]].sort_values("Match", ascending = False, ignore_index = True).head(20)

def handle_lists(query_list, doc_list):
    return [calculate_cosine_similarity(i, j) for i, j in zip(query_list, doc_list)]

app = Flask(__name__)
spark = SparkSession.builder.master("local").appName("SearchEngine").getOrCreate() # Get an existing SparkSession or create a new one
sc = spark.sparkContext

docid_list = sc.pickleFile("docid_list", 3)
corpus_tf_map = sc.pickleFile("corpus_tf").collectAsMap()
wordIdfScoreMap = sc.pickleFile("wordIdfScore").collectAsMap()
movie_metadata = load_and_clean_movie_titles()

def retrieve_movies(queries):
    query_rdd = sc.parallelize(queries)
    query_tf = query_rdd.map(generateQueryDocTf)
    print(query_tf.collect())
    multiple_word_query_tf = query_tf.map(lambda x: [(t[0], (x[1].count(t[0])/t[1])) for t in x[0]])
    multiple_word_query_tf.collect()

    # Generate the query tf-idf score
    multiple_query_tf_idf = multiple_word_query_tf.map(lambda x: [(t[0], t[1] * wordIdfScoreMap.get(t[0], 1)) for t in x])
    multiple_query_tf_idf_vector = multiple_query_tf_idf.map(lambda x: [t[1] for t in x]).collect()
    multiple_query_words = multiple_word_query_tf.map(lambda x: [t[0] for t in x]).collect()

    docid_vector = docid_list.map(lambda x : (x, [[corpus_tf_map.get((word, x), 0.0) * wordIdfScoreMap.get(word, 0.0) for word in query_word] for query_word in multiple_query_words]))

    cosine_sim_result = docid_vector.map(lambda x: (x[0], *handle_lists(multiple_query_tf_idf_vector, x[1]))).collect()
    # print(get_formatted_result_df(cosine_sim_result, multiple_word_query_tf))
    return get_formatted_result_df(cosine_sim_result, multiple_word_query_tf).to_json(orient="records")

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/find_movies', methods=['POST'])
def find_movies():
    queries = request.json['queries']
    return retrieve_movies(queries)
    return jsonify({'message': 'Movies found successfully'})

if __name__ == '__main__':
    app.run(host='0.0.0.0')
