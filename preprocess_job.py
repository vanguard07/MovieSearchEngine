import nltk
import string
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from pyspark.sql import SparkSession

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

def generateDocTf(summary):
    clean_tokens = remove_stopwords(summary)
    documentId = clean_tokens[0]
    word_docId_docLen_tuple_list = [(word, documentId, len(clean_tokens) - 1) for word in clean_tokens]
    return word_docId_docLen_tuple_list

def generateQueryDocTf(summary):
    clean_tokens = remove_stopwords(summary)
    query_docLen_tuple_list = [(word, len(clean_tokens)) for word in clean_tokens]
    return list(set(query_docLen_tuple_list)), summary.lower()

def getDocIdFromTuples(summary):
    tokens = remove_stopwords(summary)
    return tokens[0]



# Create a Spark session
spark = SparkSession.builder.master("local").appName("SearchEngine").getOrCreate() # Get an existing SparkSession or create a new one

sc = spark.sparkContext
output_format_class = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"
plots = sc.textFile("jobs/plot_summaries.txt")
plots.map(getDocIdFromTuples).saveAsPickleFile("docid_list", 3)

# Generate the corpus term frequency
corpus_tf = plots.flatMap(generateDocTf).map(lambda word_docId_docLen_tuple: ((word_docId_docLen_tuple[0], word_docId_docLen_tuple[1]), 1 / word_docId_docLen_tuple[2])).reduceByKey(lambda x, y: x + y)
# corpus_tf_map = corpus_tf.collectAsMap()
corpus_tf.saveAsPickleFile("corpus_tf", 3)

# Generate the IDF frequency
totalPlotSummaries = plots.count()
corpus_tf.map(lambda x: (x[0][0], 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], 1 + np.log(totalPlotSummaries / x[1]))).saveAsPickleFile("wordIdfScore", 3)
# wordIdfScoreMap = wordIdfScore.collectAsMap()

# # Generate the query term frequency
# queries = sc.parallelize(["Crime drama", "Action thriller", "Comedy"])
# query_tf = queries.map(generateQueryDocTf)
# multiple_word_query_tf = query_tf.map(lambda x: [(t[0], (x[1].count(t[0])/t[1])) for t in x[0]])
# query_mapping = query_tf.map(lambda x: (" ".join([t[0] for t in x[0]]), x[1])).collectAsMap()

# # Generate the query tf-idf score
# multiple_query_tf_idf = multiple_word_query_tf.map(lambda x: [(t[0], t[1] * wordIdfScoreMap.get(t[0], 1)) for t in x])
# multiple_query_tf_idf_vector = multiple_query_tf_idf.map(lambda x: [t[1] for t in x]).collect()
# multiple_query_words = multiple_word_query_tf.map(lambda x: [t[0] for t in x]).collect()

# docid_vector = docid_list.map(lambda x : (x, [[corpus_tf_map.get((word, x), 0.0) * wordIdfScoreMap.get(word, 0.0) for word in query_word] for query_word in multiple_query_words]))

# cosine_sim_result = docid_vector.map(lambda x: (x[0], *handle_lists(multiple_query_tf_idf_vector, x[1]))).collect()

# movie_metadata = load_and_clean_movie_titles()
# print(get_formatted_result_df(cosine_sim_result, multiple_word_query_tf))

# # Verify Spark version
# print("Spark version: ", spark.version)
