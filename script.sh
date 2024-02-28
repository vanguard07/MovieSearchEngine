#!/bin/bash

# Submit preprocess_job.py to Spark cluster
spark-submit --master spark://spark-master:7077 preprocess_job.py

# Submit load_and_train.py to Spark cluster
spark-submit --master spark://spark-master:7077 load_and_run.py