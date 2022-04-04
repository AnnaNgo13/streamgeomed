import sys
sys.path.append('.')

from mediator.spark_app import SparkAppWritter

syntax_trees=[
    {
        "table":[("sensor_zatu","s"),("land","l")],
        "projection":[("devEUI","s"),("land_id","l"),("data_temperature","s"),("data_airHumidity","s")],
        "group_by":["devEUI", "land_id"],
        "aggregation":[("AVG","data_temperature"), ("AVG","data_airHumidity")],
        "window": ("2minutes", "1minutes"),
        "join": [("ST_Intersects",("geometry","s"),("geometry","l"))],
        "filter": [("=",("devEUI","s"),("HE363",))]
    },
    {
        "table":[("sensor_zatu","s"),("land","l")],
        "projection":[("devEUI","s"),("land_id","l"),("data_temperature","s"),("data_airHumidity","s")],
        "join": [("ST_Intersects",("geometry","s"),("geometry","l"))],
        "filter": [("=",("devEUI","s"),("HE363",))]
    },
    {
        "table":[("sensor_zatu","s"),("land","l")],
        "projection":[("devEUI","s"),("land_id","l"),("data_temperature","s"),("data_airHumidity","s")],
        "group_by":["devEUI", "land_id"],
        "aggregation":[("AVG","data_temperature"), ("AVG","data_airHumidity")],
        "having": "devEUI=1",
        "window": ("2minutes", "1minutes"),
        "join": [("ST_Intersects",("geometry","s"),("geometry","l"))]
    },
    {
        "table":[("sensor_zatu","s")],
        "projection":[("devEUI","s"),("data_temperature","s"),("data_airHumidity","s")],
        "group_by":["devEUI"],
        "aggregation":[("AVG","data_temperature"), ("AVG","data_airHumidity")],
        "window": ("2minutes", "1minutes"),
        "filter": [("=",("devEUI","s"),("434e535301e36241",))]
    }
]

expected_query_dag=[
    [
        ("agg",(("AVG","data_temperature"), ("AVG","data_airHumidity"))),
        ("group",(("devEUI", "land_id"),("2minutes", "1minutes"))),
        ("projection",("devEUI","land_id","data_temperature","data_airHumidity")),
        ("join",("ST_Intersects",("geometry","geometry")), ("sensor_zatu","land")),
        ("filter","sensor_zatu",'devEUI="HE363"'),
        ("load","sensor_zatu"),
        ("load","land")
    ],
    [
        ("projection",("devEUI","land_id","data_temperature","data_airHumidity")),
        ("join",("ST_Intersects",("geometry","geometry")), ("sensor_zatu","land")),
        ("filter","sensor_zatu",'devEUI="HE363"'),
        ("load","sensor_zatu"),
        ("load","land"),

    ],
    [
        ("having","devEUI=1"),
        ("agg",(("AVG","data_temperature"), ("AVG","data_airHumidity"))),
        ("group",(("devEUI", "land_id"),("2minutes", "1minutes"))),
        ("projection",("devEUI","land_id","data_temperature","data_airHumidity")),
        ("join",("ST_Intersects",("geometry","geometry")), ("sensor_zatu","land")),
        ("load","sensor_zatu"),
        ("load","land")
    ],
    [
        ("agg",(("AVG","data_temperature"), ("AVG","data_airHumidity"))),
        ("group",(("devEUI",),("2minutes", "1minutes"))),
        ("projection",("devEUI","data_temperature","data_airHumidity")),
        ("filter","sensor_zatu",'devEUI="434e535301e36241"'),
        ("load","sensor_zatu"),
    ],
]

def test_query_dag():
    
    for i in range(0,len(syntax_trees)):
        spark_app_writter = SparkAppWritter(syntax_trees[i])
        spark_app = spark_app_writter.write()

        assert spark_app.print() == expected_query_dag[i]