import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from utils import generateDocTf, getDocIdFromTuples



# Create a Spark session
spark = SparkSession.builder.master("local").appName("SearchEngine").getOrCreate() # Get an existing SparkSession or create a new one

sc = spark.sparkContext
output_format_class = "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat"

plots = sc.textFile("corpus/plot_summaries.txt")
plots.map(getDocIdFromTuples).saveAsPickleFile("docid_list", 3)

# Generate the corpus term frequency
corpus_tf = plots.flatMap(generateDocTf).map(lambda word_docId_docLen_tuple: ((word_docId_docLen_tuple[0], word_docId_docLen_tuple[1]), 1 / word_docId_docLen_tuple[2])).reduceByKey(lambda x, y: x + y)
# corpus_tf_map = corpus_tf.collectAsMap()
corpus_tf.saveAsPickleFile("corpus_tf", 3)

# Generate the IDF frequency
totalPlotSummaries = plots.count()
corpus_tf.map(lambda x: (x[0][0], 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], 1 + np.log(totalPlotSummaries / x[1]))).saveAsPickleFile("wordIdfScore", 3)
