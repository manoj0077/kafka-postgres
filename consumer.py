from config import get_config
import json
from kafka import KafkaConsumer
from log import get_module_logger
import psycopg2


logger = get_module_logger(__name__, config_file_name="aiven_env.ini")


def connect_kafka_consumer(config):
    """
    Create kafka consumer which connects to kafka cluster from aiven_env config

    Args:
        config: dict object of KAFKA section from config file
    Returns:
         KafkaConsumer object connecting to cluster
    """
    logger.debug("Starting KafkaConsumer connection object creation")
    consumer = KafkaConsumer(config["TOPIC_NAME"], bootstrap_servers=config["BOOTSTRAP_SERVERS"],
                             security_protocol=config["SECURITY_PROTOCOL"], ssl_cafile=config["SSL_CAFILE"],
                             ssl_certfile=config["SSL_CERTFILE"], ssl_keyfile=config["SSL_KEYFILE"],
                             group_id=config["GROUP_ID"], client_id=config["CLIENT_ID"],
                             consumer_timeout_ms=int(config["CONSUMER_TIMEOUT_MS"]),
                             auto_offset_reset=config["AUTO_OFFSET_RESET"],
                             enable_auto_commit=False,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer


def get_db_connection(config):
    """
        Creats db connection object from values in config file

        Args:
            config: dict object of DATABASE section from config file
        Returns:
             DBConnection object
    """
    db_con = psycopg2.connect(config["URI"])
    return db_con


def update_db(records, db_config):
    """
        Takes the records and inserts into database from configuration file
        Args:
            records: List of tuples with required field data for updating table
            db_config: dict object of postgres section from configuration file
    """
    db_con = None
    query_create_person_table = '''
                    CREATE TABLE IF NOT EXISTS persons(
                        Name        VARCHAR(500),
                        Job         VARCHAR(500),
                        PhoneNumber VARCHAR(500)
                    )
                    '''
    query_insert_person = '''
                    INSERT INTO persons(Name, Job, PhoneNumber) VALUES (%s, %s, %s)
                    '''
    try:
        db_con = get_db_connection(db_config)
        cursor = db_con.cursor()
        cursor.execute(query_create_person_table)
        for record in records:
            cursor.execute(query_insert_person, record)
        db_con.commit()
        cursor.close()
    finally:
        if db_con:
            db_con.close()


def start_consumer(config_file_name):
    """ Consumes data from the given topic in env config file and inserts to postgres db
    Args:
        config_file_name: configuration file name
    """
    consumer = None
    try:
        config = get_config(config_file_name)
        logger.debug("Consuming data from the topic {}".format(config["KAFKA"]["TOPIC_NAME"]))
        consumer = connect_kafka_consumer(config["KAFKA"])
        records = []
        for msg in consumer:
            logger.debug(msg)
            record_to_insert = (msg.value['name'], msg.value['job'], msg.value['phone_number'])
            records.append(record_to_insert)
        update_db(records, config["POSTGRES"])
        consumer.commit()
        logger.info("Messages consumed successfully")
    except Exception as e:
        logger.error("Exception in consuming the message: {}".format(e))
    finally:
        if consumer:
            consumer.close()


def main():
    start_consumer(config_file_name="aiven_env.ini")


if __name__ == "__main__":
    main()
