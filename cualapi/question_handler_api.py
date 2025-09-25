from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
import json
import csv

app = Flask(__name__)

# Conexión a Elasticsearch (ajusta con tus credenciales y host)
es = Elasticsearch(hosts=["http://localhost:9200"],
                   http_auth=("elastic", "prontotodoacabara"))  

with open("../questions/questions.json", "r", encoding="utf-8") as f:
    preguntas_data = json.load(f)

frecuentes = {k.lower(): v for k, v in preguntas_data.get("common", {}).items()}
infrecuentes = {k.lower(): v for k, v in preguntas_data.get("uncommon", {}).items()}


@app.route("/request", methods=["POST"])
def request():
    data = request.json
    query = data.get("query", "").lower()
    if query in frecuentes:
        with open("../common_questions/answers.csv", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)  
            for row in reader:
                pregunta = row["pregunta"].lower()
                respuesta = row["respuesta"]
                if query == pregunta:
                    return jsonify({
                        "source": "faq-frecuente",
                        "response": respuesta
                    })
    if query in infrecuentes:
        res = es.search(index="data", **query)
        return jsonify({
             "source": "elasticsearch",
             "response": res
            })

    return jsonify({
        "source": "none",
        "response": "Lo siento, no tengo acceso a esa información."
    })

if __name__ == "__main__":
    app.run(debug=True, port=5000)