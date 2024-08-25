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
import uuid
from datetime import datetime
# import os
# import tempfile
# import gzip
# import shutil

app = Flask(__name__)

kafka_producer = KafkaProducer(
    bootstrap_servers="3.36.83.5:9092",
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    max_request_size=10485760
)

KAFKA_TOPIC = 'pothole-detection'

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

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "UP"}), 200


@app.route('/detect', methods=['POST'])
def detect():
    # 로그 기록
    logger = set_logger('./pothole-kafka.log')

    if 'video' not in request.files:
        return 'No video in the request', 400

    lat = request.form.get('lat')
    lon = request.form.get('lon')
    video = request.files.get('video')
    video_string = base64.b64encode(video.read()).decode('utf-8')

    # # 임시 파일 생성 및 압축
    # with tempfile.NamedTemporaryFile(delete=True) as temp_video:
    #     video.save(temp_video.name)
    #     compressed_file_path = compress_file(temp_video.name, 'video.mp4.gz')
    #
    # # 압축된 파일 읽기
    # with open(compressed_file_path, 'rb') as f:
    #     video_zip = f.read()
    #
    # base64.b64encode(video_zip).decode('utf-8')

    # 메세지 생성
    current_time = datetime.now().strftime('%Y%m%d%H%M%S%f')
    UUID = str(uuid.uuid5(uuid.NAMESPACE_DNS, current_time))

    kafka_msg = {
        'id': UUID,
        'content': {
            'lat': lat,  # 위도
            'lon': lon,  # 경도
            'video': video_string
        }
    }

    # kafka에 전송
    try:
        future = kafka_producer.send(topic=KAFKA_TOPIC, value=kafka_msg)

        future.add_callback(lambda metadata: logger.info(
            f"Message {[UUID]} sent to {[metadata.topic]} Success!"))

        future.add_errback(lambda e: logger.error(str(e)))
        return jsonify({"status": "OK"}), 200

    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {str(e)}")
        return jsonify({"status": "FAIL"}), 400


# def compress_file(input_file, output_file):
#     with open(input_file, 'rb') as f_in:
#         with gzip.open(output_file, 'wb') as f_out:
#             shutil.copyfileobj(f_in, f_out)
#     return output_file


if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)