FROM spark:3.5.0-python3

USER root

WORKDIR /app

# Create a new folder
RUN mkdir /app/jobs

# Install dependencies from requirements.txt
COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Copy the text files to the container
COPY corpus/plot_summaries.txt /app/jobs/plot_summaries.txt
COPY corpus/movie.metadata.tsv /app/jobs/movie_metadata.tsv

# Copy your PySpark script
COPY preprocess_job.py /app/jobs/preprocess_job.py
COPY load_and_run.py /app/jobs/load_and_run.py

RUN mkdir /app/jobs/templates
COPY templates/index.html /app/jobs/templates/index.html

# Copy the script to the container
RUN mkdir /app/scripts
COPY script.sh /app/scripts/script.sh
RUN chmod +x /app/scripts/script.sh

# Run the script with spark-submit
CMD ["/app/scripts/script.sh"]