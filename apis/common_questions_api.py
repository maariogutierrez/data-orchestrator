from elasticsearch import Elasticsearch
import csv
import json
from logger import get_logger
import os

logger = get_logger(__name__)

# Initialize the Elasticsearch client
es = Elasticsearch(hosts=["http://localhost:9200"],
                   http_auth=("elastic", "prontotodoacabara"))  

def common_questions(index):
    logger.info("Starting common questions for index: %s", index)
    
    # Load common questions from JSON file
    questions_path = os.path.join(os.path.dirname(__file__), "../questions/questions.json")
    with open(questions_path, "r", encoding="utf-8") as f:
        questions_data = json.load(f)
    common_questions = questions_data.get("common", {})

    results = []
    for question, query_data in common_questions.items():
        query = query_data.get("query", {})
        try:
            response = es.search(index=index, **query)
            # Try to extract aggregation or count results
            results.append([question, response])
            logger.info("Question: %s | Result: %s", question, response)
        except Exception as e:
            logger.error("Error processing question '%s': %s", question, str(e))
            results.append([question, "Error"])

    # Save results to CSV
    csv_path = os.path.join(os.path.dirname(__file__), "../common_questions/answers.csv")
    with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Question', 'Answer'])
        writer.writerows(results)
    logger.info("Results saved to %s", csv_path)

if __name__ == "__main__":
    index_name = "data"
    common_questions(index_name)