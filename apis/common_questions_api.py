from elasticsearch import Elasticsearch
import csv
from logger import get_logger

logger = get_logger(__name__)

# Initialize the Elasticsearch client
es = Elasticsearch(hosts=["http://localhost:9200"],
                   http_auth=("elastic", "prontotodoacabara"))  

def common_questions(index):
    logger.info("Starting common questions for index: %s", index)
    
    # Access the JSON file of common questions


    # Save results to CSV

if __name__ == "__main__":
    index_name = "data"
    common_questions(index_name)