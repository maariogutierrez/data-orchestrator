# This files contains your custom actions which can be used to run
# custom Python code.
#
# See this guide on how to implement these action:
# https://rasa.com/docs/rasa/custom-actions


# This is a simple example for a custom action which utters "Hello World!"

from typing import Any, Text, Dict, List

from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher

from elasticsearch import Elasticsearch
import csv
import json
import os

# Initialize the Elasticsearch client
es = Elasticsearch(hosts=["http://elasticsearch:9200"])

questions_path = os.path.join(os.path.dirname(__file__), "../../questions/questions.json")
with open(questions_path, "r", encoding="utf-8") as f:
    questions_data = json.load(f)
uncommon_questions = questions_data.get("uncommon", {})

class NumberofTrips(Action):

    def name(self) -> Text:
        return "action_number_of_trips"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        with open("../../common_questions/answers.csv", mode='r', encoding='utf-8') as archivo:
            lector = csv.reader(archivo)
            for fila in lector:
                if fila and fila[0] == "Number of trips":  
                    answer = fila[1]
            
        dispatcher.utter_message(text=f"El número de viajes total es {answer}")
        
        return []
