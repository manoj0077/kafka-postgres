# kafka-postgres
sample to consume aiven kafka and postgres service

This is a sample project in python which gives reference for consuming kafka and postgres services from aiven.

# Pre-Reqs
* Create python venv and install dependencies using requirements.txt file in the repo.
* Sign up for kafka and postgres service on aiven.
* Modify the aiven_env.ini and aiven_test_env.ini files accordingly for dev and test environment correspondingly. Kafka uses SSL authorization.

## Configuration Parameters

[DEFAULT]  
LOG_LEVEL = <DEBUG | INFO>  
LOG_FILE = < Log File Location >  

[KAFKA]  
AUTO_OFFSET_RESET = < earliest | latest >  
BOOTSTRAP_SERVERS = < host:port >  
CLIENT_ID = < sample consumer name >  
CONSUMER_TIMEOUT_MS = 5000  
GROUP_ID = < sample group name >  
SECURITY_PROTOCOL = SSL  
SSL_CAFILE = < ca.pem file location >  
SSL_CERTFILE = < service.cert file location >  
SSL_KEYFILE = < service.key file location >  
TOPIC_NAME = < sample topic name >  

[POSTGRES]  
URI = postgres://\<user>:\<prod>@\<HOST>:\<PORT>/\<DBNAME>?sslmode=require  


Producer and Consumer are the two main components.

### Producer:
* Generates random data of 10 samples
* Sends it to the kafka broker. Success and Error callbacks are just used for logging
* Run the producer using "python producer.py"

### Consumer:
* Consumes the data from the given topic in configuraiton file
* Sends the data to the postgres database mentioned in the configuraiton file
* Run the consumer using "python consumer.py"

### Test Case Execution
* Ensure  aiven_test_env.ini is updated
* Run the test suite using "python -m unittest test_aiven.py"

### References
* https://help.aiven.io/en/articles/489572-getting-started-with-aiven-kafka
* https://kafka-python.readthedocs.io/en/master/usage.html
* https://help.aiven.io/en/articles/489573-getting-started-with-aiven-postgresql
* https://pynative.com/python-postgresql-tutorial/