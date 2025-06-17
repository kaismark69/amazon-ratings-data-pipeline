from flask import Flask, send_file, jsonify
import os

app = Flask(__name__)
OUTPUT_DIR = "/output"

@app.route("/")
def index():
    return jsonify({"message": "Delivery Microservice is running."})

@app.route("/data/<int:month>")
def get_monthly_aggregated(month):
    file_path = os.path.join(OUTPUT_DIR, f"aggregated_ratings_month_{month}.csv")
    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404
    # send the file as a response in CSV format
    return send_file(file_path, mimetype="text/csv")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

