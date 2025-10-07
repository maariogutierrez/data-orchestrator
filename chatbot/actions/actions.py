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
import ast

from .location_mapping import location_mapping

# Initialize the Elasticsearch client
es = Elasticsearch(hosts=["http://elasticsearch:9200"])


def extract_value(question, response: dict):
    match question:
        case "Average trip duration":
            return response.get("aggregations", {}).get("avg_trip_duration", {}).get("value", 0)
        case "Trips per pickup location":
            buckets = response.get("aggregations", {}).get("trips_per_pu", {}).get("buckets", [])
            return {bucket['key']: bucket['doc_count'] for bucket in buckets}
        case "Average tip per payment type":
            buckets = response.get("aggregations", {}).get("payment_type", {}).get("buckets", [])
            return {bucket['key']: bucket['tip_amount']['value'] for bucket in buckets}
        case "Revenue per vendor":
            buckets = response.get("aggregations", {}).get("revenue_vendor", {}).get("buckets", [])
            return {bucket['key']: bucket['total_revenue']['value'] for bucket in buckets}
        case _:
            return response



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

class TripsperVendor(Action):

    def name(self) -> Text:
        return "action_trips_per_vendor"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        with open("../../common_questions/answers.csv", mode='r', encoding='utf-8') as archivo:
            lector = csv.reader(archivo)
            for fila in lector:
                if fila and fila[0] == "Trips per vendor":  
                    answer = fila[1]
            
        dispatcher.utter_message(text=f"El proveedor 1 ha realizado {ast.literal_eval(answer).get(1, 0)} viajes y el proveedor 2 ha realizado {ast.literal_eval(answer).get(2, 0)} viajes")
        
        return []


class TripsperPaymentType(Action):

    def name(self) -> Text:
        return "action_trips_per_payment_type"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        with open("../../common_questions/answers.csv", mode='r', encoding='utf-8') as archivo:
            lector = csv.reader(archivo)
            for fila in lector:
                if fila and fila[0] == "Trips per payment type":  
                    answer = fila[1]
            
        dispatcher.utter_message(text=f"Se han realizado {ast.literal_eval(answer).get(1, 0)} viajes con tipo de pago 1, {ast.literal_eval(answer).get(2, 0)} viajes con tipo de pago 2, {ast.literal_eval(answer).get(3, 0)} viajes con pago con tipo de pago 3 y {ast.literal_eval(answer).get(4, 0)} viajes con tipo de pago 4")
        
        return []
    
class AverageTripDistance(Action):

    def name(self) -> Text:
        return "action_average_trip_distance"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        with open("../../common_questions/answers.csv", mode='r', encoding='utf-8') as archivo:
            lector = csv.reader(archivo)
            for fila in lector:
                if fila and fila[0] == "Average trip distance":  
                    answer = fila[1]
            
        dispatcher.utter_message(text=f"La distancia media recorrida por viaje es de {round(float(answer),2)}km")
        
        return []
    
class TotalRevenue(Action):

    def name(self) -> Text:
        return "action_total_revenue"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        with open("../../common_questions/answers.csv", mode='r', encoding='utf-8') as archivo:
            lector = csv.reader(archivo)
            for fila in lector:
                if fila and fila[0] == "Total revenue":  
                    answer = fila[1]
            
        dispatcher.utter_message(text=f"Los ingresos totales generados por los viajes son de {round(float(answer),2)}€")
        
        return []
    
class AveragePassengersperTrip(Action):

    def name(self) -> Text:
        return "action_average_passengers_per_trip"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        with open("../../common_questions/answers.csv", mode='r', encoding='utf-8') as archivo:
            lector = csv.reader(archivo)
            for fila in lector:
                if fila and fila[0] == "Average passengers per trip":  
                    answer = fila[1]
            
        dispatcher.utter_message(text=f"El número medio de pasajeros por viaje es de {round(float(answer),2)} personas")
        
        return []   
    
class AverageTripDuration(Action):

    def name(self) -> Text:
        return "action_average_trip_duration"

    def run(self, dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        intent = "Average trip duration"
        questions_path = os.path.join(os.path.dirname(__file__), "../../questions/questions.json")
        with open(questions_path, "r", encoding="utf-8") as f:
            questions_data = json.load(f)
        uncommon_questions = questions_data.get("uncommon", {})
        for question, query_data in uncommon_questions.items():
            if question==intent:
                query = query_data.get("query", {})
                try:
                    response = es.search(index="data",body=query)
                    value = round(float(extract_value(question, response)),2)
                except Exception as e:
                    value="Error"
        dispatcher.utter_message(text=f"La duración media de los trayectos es de {value} minutos")
    
        return []
    
class TripsperPickupLocation(Action):

    def name(self) -> Text:
        return "action_trips_per_pickup_location"

    def run(self, dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        intent = "Trips per pickup location"
        questions_path = os.path.join(os.path.dirname(__file__), "../../questions/questions.json")
        with open(questions_path, "r", encoding="utf-8") as f:
            questions_data = json.load(f)
        uncommon_questions = questions_data.get("uncommon", {})
        for question, query_data in uncommon_questions.items():
            if question==intent:
                query = query_data.get("query", {})
                try:
                    response = es.search(index="data",**query)
                    value = extract_value(question, response)
                except Exception as e:
                    value="Error"
        message_parts = []
        for i, key in enumerate(list(value.keys())[:10]):  
            street_name = location_mapping.get(int(key), f"Localización {key}")
            message_parts.append(f"{street_name}: {value.get(key, 0)} viajes")

        message_text = "La distribución de viajes por las 10 localizaciones de recogida más comunes es la siguiente: " + ". ".join(message_parts) + "."
        dispatcher.utter_message(text=message_text)        
        return []
    
class AverageTipperPaymentType(Action):

    def name(self) -> Text:
        return "action_average_tip_per_payment_type"

    def run(self, dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        intent = "Average tip per payment type"
        questions_path = os.path.join(os.path.dirname(__file__), "../../questions/questions.json")
        with open(questions_path, "r", encoding="utf-8") as f:
            questions_data = json.load(f)
        uncommon_questions = questions_data.get("uncommon", {})
        for question, query_data in uncommon_questions.items():
            if question==intent:
                query = query_data.get("query", {})
                try:
                    response = es.search(index="data",**query)
                    value = response
                except Exception as e:
                    value = "Error"
        dispatcher.utter_message(text=f"Medias de propina: método 1: {round(float(value.get('aggregations', {}).get('avg_tip_payment', {}).get('buckets', [{}])[0].get('avg_tip', {}).get('value', 0)), 2)}€, método 2: {round(float(value.get('aggregations', {}).get('avg_tip_payment', {}).get('buckets', [{}])[1].get('avg_tip', {}).get('value', 0)), 2)}€, método 3: {round(float(value.get('aggregations', {}).get('avg_tip_payment', {}).get('buckets', [{}])[2].get('avg_tip', {}).get('value', 0)), 2)}€, método 4: {round(float(value.get('aggregations', {}).get('avg_tip_payment', {}).get('buckets', [{}])[3].get('avg_tip', {}).get('value', 0)), 2)}€.")

        return []
    
class RevenueperVendor(Action):

    def name(self) -> Text:
        return "action_revenue_per_vendor"

    def run(self, dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        intent = "Revenue per vendor"
        questions_path = os.path.join(os.path.dirname(__file__), "../../questions/questions.json")
        with open(questions_path, "r", encoding="utf-8") as f:
            questions_data = json.load(f)
        uncommon_questions = questions_data.get("uncommon", {})
        for question, query_data in uncommon_questions.items():
            if question==intent:
                query = query_data.get("query", {})
                try:
                    response = es.search(index="data",**query)
                    value = extract_value(question, response)
                except Exception as e:
                    value="Error" 

        dispatcher.utter_message(text=f"El proveedor 1 ha obtenido {round(float(value.get(1, 0)),2)}€ de beneficio y el proveedor 2 ha obtenido {round(float(value.get(2, 0)), 2)}€")        
        return []

class ActionDefaultFallback(Action):

    def name(self) -> Text:
        return "action_default_fallback"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:

        dispatcher.utter_message(text="Lo siento, no tienes permiso para ver esa información o no entiendo tu consulta.")
        return []