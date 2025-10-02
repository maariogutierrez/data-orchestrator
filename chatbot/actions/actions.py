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


def extract_value(response: dict):
    """Extrae el valor más relevante de la respuesta de Elasticsearch."""
    # Si hay agregaciones, devuelve el primer valor/buckets
    if "aggregations" in response:
        agg = response["aggregations"]
        # Tomar la primera agregación
        agg_name, agg_data = next(iter(agg.items()))
        if "value" in agg_data:
            return agg_data["value"]  # Ej: promedio, suma, etc.
        if "buckets" in agg_data:
            return agg_data["buckets"]  # Lista de buckets
        return agg_data

    # Si no hay agregaciones, devolver el total de hits
    if "hits" in response and "total" in response["hits"]:
        return response["hits"]["total"]["value"]

    # Si nada aplica, devolver todo
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
            
        dispatcher.utter_message(text=f"El número de viajes por conductor es {answer}")
        
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
            
        dispatcher.utter_message(text=f"El número de viajes por tipo de pago son: {answer}")
        
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
            
        dispatcher.utter_message(text=f"La distancia media recorrida por viaje es de {answer} kilómetros/viaje")
        
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
            
        dispatcher.utter_message(text=f"Los ingresos totales generados por los viajes son de {answer} €")
        
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
            
        dispatcher.utter_message(text=f"El número medio de pasajeros por viaje es de {answer} personas")
        
        return []
    
class MaxTripDistance(Action):
    
    def name(self) -> Text:
        return "action_max_trip_distance"

    def run(self, dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        intent = "Max trip distance"
        questions_path = os.path.join(os.path.dirname(__file__), "../../questions/questions.json")
        with open(questions_path, "r", encoding="utf-8") as f:
            questions_data = json.load(f)
        uncommon_questions = questions_data.get("uncommon", {})
        for question, query_data in uncommon_questions.items():
            if question==intent:
                query = query_data.get("query", {})
                try:
                    response = es.search(index="data",**query)
                    value = extract_value(response)
                except Exception as e:
                    value="Error"
        dispatcher.utter_message(text=f"La máxima distancia recorrida en un viaje ha sido de {value} kilómetros")
    
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
                    response = es.search(index="data",**query)
                    value = extract_value(response)
                except Exception as e:
                    value="Error"
        dispatcher.utter_message(text=f"La duración media de los trayectos es de {int(value)/60} minutos")
    
        return []
    
class TripsperPickupLocation(Action):

    def name(self) -> Text:
        return "action_trips_per_pickup_location"

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
                    response = es.search(index="data",**query)
                    value = extract_value(response)
                except Exception as e:
                    value="Error"
        dispatcher.utter_message(text=f"El número de viajes por localización de recogida son: {value}")
        
        return []
    
class AverageTipperPaymentType(Action):

    def name(self) -> Text:
        return "action_average_tip_per_payment_type"

    def run(self, dispatcher: CollectingDispatcher,
        tracker: Tracker,
        domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        intent = "Average tip per payment"
        questions_path = os.path.join(os.path.dirname(__file__), "../../questions/questions.json")
        with open(questions_path, "r", encoding="utf-8") as f:
            questions_data = json.load(f)
        uncommon_questions = questions_data.get("uncommon", {})
        for question, query_data in uncommon_questions.items():
            if question==intent:
                query = query_data.get("query", {})
                try:
                    response = es.search(index="data",**query)
                    value = extract_value(response)
                except Exception as e:
                    value="Error"
        dispatcher.utter_message(text=f"Las propinas medias por tipos de pago son de {value} €")
    
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
                    value = extract_value(response)
                except Exception as e:
                    value="Error" 

        dispatcher.utter_message(text=f"Los ingresos generados por cada conductor son {value} € de media")
        
        return []
    
class NoAcceso(Action):

    def name(self) -> Text:
        return "action_no_acceso"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
            
        dispatcher.utter_message(text=f"Lo siento, no tengo acceso a esa información.")
        
        return []