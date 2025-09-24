from elasticsearch import Elasticsearch
import csv
from logger import get_logger

logger = get_logger(__name__)

# Initialize the Elasticsearch client
es = Elasticsearch(hosts=["http://localhost:9200"],
                   http_auth=("elastic", "prontotodoacabara"))  

def common_questions(index):
    logger.info("Starting common questions for index: %s", index)
    #1. Number of trips
    result = es.count(index=index)
    count = result.get('count', 0)
    logger.info("Number of trips: %d", count)

    #2. Trips per vendor
    trips_per_vendor = es.search(
        index=index,
        size=0,
        aggs={
            "trips_per_vendor": {"terms": {"field": "VendorID"}}
        }
    )
    logger.info("Trips per vendor: %s", trips_per_vendor)

    #3. Average trip distance
    average_distance = es.search(
        index=index,
        size=0,
        aggs={"average_distance": {"avg": {"field": "trip_distance"}}}
    )
    logger.info("Average trip distance: %s", average_distance)

    # Save results to CSV
    csv_path = "../common_questions/answers.csv"
    with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Question', 'Answer'])
        writer.writerow(['Number of trips', count])
        writer.writerow(['Trips per vendor', trips_per_vendor])
        writer.writerow(['Average trip distance', average_distance])
        logger.info("Results saved to %s", csv_path)

if __name__ == "__main__":
    index_name = "data"
    common_questions(index_name)