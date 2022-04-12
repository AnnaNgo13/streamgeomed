# A mediator for real-time spatio-temporal data integration and analysis with Apache Spark 

## Getting started

### Prerequisites for the project

1. Install requirements
```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt 
```

2. A running Apache Spark cluster. Set:

```
export SPARK_MASTER=spark://IP_ADDRESS:PORT // by defaut set to spark://127.0.0.1:7077
```


### Run example

1. Populate kafka topic

For setting up Kafka cluster
```
docker-compose -f data_in_shipper/docker-compose.yml up -d
```

Then, 
```
KAFKA_BROKER=IP_ADDRESS:PORT python data_in_shipper/populate_kafka.py zatu
```

2. Run system with example query
```
python tests/mediator/e2e.py
```

### Run unit tests

```
pytest .
```
