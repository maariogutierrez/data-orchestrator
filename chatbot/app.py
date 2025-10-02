from flask import Flask, request, jsonify, render_template
import requests

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/chat", methods=["POST"])
def chat():
    user_message = request.json.get("message")
    rasa_response = requests.post(
        "http://localhost:5005/webhooks/rest/webhook",
        json={"message": user_message}
    )
    bot_response = rasa_response.json()
    if bot_response:
        bot_message = bot_response[0].get("text", "Lo siento, no puedo responder eso en este momento.")
    else:
        bot_message = "Lo siento, no entendí tu mensaje."
    return jsonify({"bot_response": bot_message})

if __name__ == "__main__":
    app.run(port=8000, debug=True)
