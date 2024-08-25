#https://velog.io/@joon6093/%EA%B3%B5%EB%B6%80%EC%A0%95%EB%A6%AC-Flask%EC%97%90%EC%84%9C-eureka-kafka-logging-%EC%82%AC%EC%9A%A9%ED%95%98%EA%B8%B0

# pip install kafka-python
# pip install rich.logging

import base64
from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import logging
import logging.handlers
from rich.logging import RichHandler

app = Flask(__name__)

RICH_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
FILE_HANDLER_FORMAT = "[%(asctime)s]\\t%(levelname)s\\t[%(filename)s:%(funcName)s:%(lineno)s]\\t>> %(message)s"

def set_logger(log_path) -> logging.Logger:
    logging.basicConfig(
        level="NOTSET",
        format=RICH_FORMAT,
        handlers=[RichHandler(rich_tracebacks=True)]
    )
    logger = logging.getLogger("rich")

    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(FILE_HANDLER_FORMAT))
    logger.addHandler(file_handler)

    return logger


kafka_producer = KafkaProducer(
    bootstrap_servers="3.36.83.5:9092",
    max_request_size=10000000
    #value_serializer=lambda x: json.dumps(x).encode('utf-8'),
)

KAFKA_TOPIC = 'pothole-detection'


@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "UP"}), 200


@app.route('/detect', methods=['POST'])
def detect():
    logger = set_logger('./demo.log')

    if 'images' not in request.files:
        return 'No images part in the request', 400

    # lat = request.form.get('lat')
    # lon = request.form.get('lon')
    # importance = request.form.get('importance')
    # dangerous = request.form.get('dangerous')
    # images = request.files.getlist('images')
    images = request.files.get('images')

    # image_data_list = []

    # for image_file in images:
    #     image_data = image_file.read()
    #     image_data_list.append(base64.b64encode(image_data).decode('utf-8'))

    kafka_message = {
        # 'lat': lat,
        # 'lon': lon,
        # 'importance': importance,
        # 'dangerous': dangerous,
        # 'images': image_data_list
        'images': images.read()
    }

    try:
        future = kafka_producer.send(topic=KAFKA_TOPIC, value=kafka_message)

        future.add_callback(lambda metadata: logger.info(
            f"Message sent to {metadata.topic} Success!"))
        future.add_errback(lambda e: logger.error(str(e)))
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {str(e)}")

    return jsonify({"status": "OK"}), 200


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)