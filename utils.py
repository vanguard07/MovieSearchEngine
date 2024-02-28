import string
import nltk
import numpy as np
from nltk.corpus import stopwords

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

def generateDocTf(summary):
    clean_tokens = remove_stopwords(summary)
    documentId = clean_tokens[0]
    word_docId_docLen_tuple_list = [(word, documentId, len(clean_tokens) - 1) for word in clean_tokens]
    return word_docId_docLen_tuple_list

def getDocIdFromTuples(summary):
    tokens = remove_stopwords(summary)
    return tokens[0]