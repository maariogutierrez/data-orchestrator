from elasticsearch import Elasticsearch
import csv
import json
from logger import get_logger
import os
import time

logger = get_logger(__name__)

# Initialize the Elasticsearch client
es = Elasticsearch(hosts=["http://elasticsearch:9200"])  


def extract_value(question, response: dict):
    match question:
        case "Number of trips":
            return response.get('hits', {}).get('total', {}).get('value', 0)
        case "Trips per vendor":
            buckets = response.get('aggregations', {}).get('trips_per_vendor', {}).get('buckets', [])
            return {bucket['key']: bucket['doc_count'] for bucket in buckets}
        case "Trips per payment type":
            buckets = response.get('aggregations', {}).get('trips_per_payment', {}).get('buckets', [])
            return {bucket['key']: bucket['doc_count'] for bucket in buckets}
        case "Average trip distance":
            return response.get('aggregations', {}).get('avg_distance', {}).get('value', 0)
        case "Total revenue":
            return response.get('aggregations', {}).get('total_revenue', {}).get('value', 0)
        case "Average passengers per trip":
            return response.get('aggregations', {}).get('avg_passengers', {}).get('value', 0)
        case _:
            return response

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
        response = es.search(index=index, **query)
        value = extract_value(question, response)  
        results.append([question, value])
        logger.info("Question: %s | Result: %s", question, value)

    # Save results to CSV
    csv_path = os.path.join(os.path.dirname(__file__), "../common_questions/answers.csv")
    with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Question', 'Answer'])
        writer.writerows(results)
    logger.info("Results saved to %s", csv_path)


if __name__ == "__main__":
    index_name = "data"
    try:
        common_questions(index_name)
    except:
        logger.info("There was an error. Trying again in 10 seconds...")
        time.sleep(10)
        common_questions(index_name)