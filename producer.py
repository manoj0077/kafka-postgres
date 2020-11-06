from config import get_config
import json
from kafka import KafkaProducer
from log import get_module_logger
from random_data import random_data


logger = get_module_logger(__name__, config_file_name="aiven_env.ini")


def success_handler(event_metadata):
    logger.debug("Message send successful for {}".format(event_metadata))


def error_handler(excp):
    logger.error('Failed to send the message', exc_info=excp)


def connect_kafka_producer(config):
    """
    Create kafka producer which connects to kafka cluster from aiven_env config

    Args:
        config: dict object of KAFKA section from config file
    Returns:
         KafkaProducer object connecting to cluster
    """
    logger.debug("Starting KafkaProducer connection object creation")
    producer = KafkaProducer( bootstrap_servers=config["BOOTSTRAP_SERVERS"],
                              security_protocol=config["SECURITY_PROTOCOL"], ssl_cafile=config["SSL_CAFILE"],
                              ssl_certfile=config["SSL_CERTFILE"], ssl_keyfile=config["SSL_KEYFILE"],
                              value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    return producer


def start_producer(count, config_file_name):
    """ Generates random data for given count persons, creates kafka producer and send messages to a topic.

    Args:
        count: number of events to be produced
        config_file_name: configuration file name
    """
    producer = None
    try:
        config = get_config(config_file_name)
        producer = connect_kafka_producer(config['KAFKA'])

        logger.debug("Generating random data for {} persons".format(count))
        data = random_data(count)

        logger.debug("Sending generated random data")
        for person_info in data:
            logger.debug("Person Data: {}".format(person_info))
            producer.send(config['KAFKA']['TOPIC_NAME'], person_info).add_callback(success_handler).\
                add_errback(error_handler)
        producer.flush()
        logger.info("Messages sent successfully")
    except Exception as e:
        logger.error("Exception in producing the message: {}".format(e))
    finally:
        if producer:
            producer.close()


def main():
    # produces 10 events based on configuration from aiven_env.ini
    start_producer(count=10, config_file_name="aiven_env.ini")


if __name__ == "__main__":
    main()
