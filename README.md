This repository contains the PySpark code for building a search engine for movie plot summaries using the tf-idf technique. The backend of the search engine is implemented using Flask, which exposes an API. This API accepts user queries and returns a list of possible movies based on the search query.

## Description
The goal is to create a search engine that allows users to search for movies based on their plot summaries. The system utilizes the tf-idf technique to identify relevant movie summaries based on the user's search query.

## Getting Started

#### 1. Data Upload
	- Download the corpus from http://www.cs.cmu.edu/~ark/personas/data/MovieSummaries.tar.gz.
	- Extract and add the **plot_summaries.txt** and **movie.metadata.tsv** file to the **corpus** folder to the repo.
#### 2. Build the docker container
```docker
docker build -t search_engine .
```
#### 3. Run the container
```docker
docker run --rm -p 3000:5000 -it search_engine
```
#### 4. Final steps
- Go to `http://0.0.0.0:3000/` and enter your search query
## Technologies Used
- [PySpark](https://pypi.org/project/pyspark/) 
- [Flask](https://flask.palletsprojects.com/en/3.0.x/)
- [Docker](https://www.docker.com)

## License
This project is licensed under the [MIT License](LICENSE).