"""Receiver service for processing air quality and weather data through Kafka"""

import datetime
import json
import logging
import logging.config
import uuid

import connexion
from connexion import NoContent
import yaml
from kafka import KafkaProducer

with open('app_config.yml', 'r', encoding='utf-8') as f:
    app_config = yaml.safe_load(f.read())

with open('log_config.yml', 'r', encoding='utf-8') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

producer = KafkaProducer(
    bootstrap_servers=f"{app_config['events']['hostname']}:{app_config['events']['port']}",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def submit_air_quality_data(body):
    """ Forwards air quality data to Kafka """
    trace_id = str(uuid.uuid4())
    logger.info("Received event air quality request with a trace id of %s", trace_id)
    
    body['trace_id'] = trace_id
    
    msg = {
        "type": "air_quality",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": body
    }
    producer.send(app_config['events']['topic'], value=msg)
    
    logger.info("Returned event air quality response (Id: %s) with status 201", trace_id)
    return NoContent, 201

def submit_weather_data(body):
    """ Forwards weather data to Kafka """
    trace_id = str(uuid.uuid4())
    logger.info("Received event weather request with a trace id of %s", trace_id)
    
    body['trace_id'] = trace_id
    
    msg = {
        "type": "weather",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": body
    }
    producer.send(app_config['events']['topic'], value=msg)
    
    logger.info("Returned event weather response (Id: %s) with status 201", trace_id)
    return NoContent, 201

app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
