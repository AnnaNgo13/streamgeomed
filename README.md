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






















### Prerequisites for running in Single node

for libraries path
```
export SPARK_HOME=/home/alami/app/spark-3.1.2-bin-hadoop3.2/
export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH
export PYTHONPATH=/home/alami/anna/work2_draft:$PYTHONPATH
```
for anna
```
export SPARK_HOME=/Users/annango/app/spark-3.1.2-bin-hadoop3.2/
export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH
export PYTHONPATH=/Users/annango/code/work2_draft:$PYTHONPATH
```
for limos
```
export SPARK_HOME=/home/ttngo/app/spark-3.1.2-bin-hadoop3.2/
export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH
export PYTHONPATH=/home/ttngo/code/work2_draft:$PYTHONPATH
```

optional (in case you have error Pyspark worker and driver do not have same python version)
```
export PYSPARK_PYTHON=python3.8
export PYSPARK_DRIVER_PYTHON=python3.8
```

### Run the Mediator

#### schema1

1. Run kafka in docker (run in different terminal)
```
docker-compose -f data_in_shipper/docker-compose.yml up -d
```

2. Populate kafka topic (in another terminal)
```
export PYTHONPATH=/home/alami/anna/work2_draft
python data_in_shipper/populate_kafka.py zatu
```
for anna
```
export PYTHONPATH=/Users/annango/code/work2_draft
python3 data_in_shipper/populate_kafka.py zatu
```

3. Run wrapper with passing schema as argument
```
python3 mediator/wrapper.py schema1
```

Query example

SELECT * FROM sensor_land

#### schema2

1. Run kafka in docker
```
docker-compose -f data_in_shipper/docker-compose.yml up -d
```

2. Populate kafka topic 

In one terminal
```
export PYTHONPATH=/home/alami/anna/work2_draft
python data_in_shipper/populate_kafka.py zatu
```
In another terminal
```
export PYTHONPATH=/home/alami/anna/work2_draft
python data_in_shipper/populate_kafka.py aydat
```

3. Run wrapper with passing schema as argument
```
python mediator/wrapper.py schema2
```

Query example

SELECT * FROM sensor AS s, land AS l WHERE ST_Intersects(s.geometry, l.geometry)

### Run pre-defined queries (for testing)

1. To run example with static input data
```
python mediator/queries_example/sql_join_query_1.py
```

2. To run example with kafka stream 

2.1. Run kafka in docker
```
docker-compose -f data_in_shipper/docker-compose.yml up -d
```

2.2. Populate kafka topic (in another terminal)
```
export PYTHONPATH=/home/alami/anna/work2_draft
python data_in_shipper/populate_kafka.py zatu
```

2.3. Run spark job

```
python mediator/queries_example/sql_cont_join_query_1.py
python mediator/queries_example/sql_query_windowed.py
python mediator/queries_example/sql_cont_join_query_windowed.py
```
