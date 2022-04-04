import sys
sys.path.append('.')

from mediator.query_parser import Parser, where_parser

queries = [
            """
                SELECT s.devEUI, l.land_id, AVG(s.data_temperature), AVG(s.data_airHumidity)
                FROM sensor_zatu AS s, land AS l
                WHERE ST_Intersects(s.geometry, l.geometry) AND s.devEUI=HE363
                GROUP BY devEUI, land_id WINDOW 2minutes 1minutes
            """,
            """
                SELECT s.devEUI, l.land_id, s.data_temperature, s.data_airHumidity
                FROM sensor_zatu AS s, land AS l
                WHERE ST_Intersects(s.geometry, l.geometry) AND s.devEUI=HE363
            """,
            """
                SELECT s.devEUI, l.land_id, AVG(s.data_temperature), AVG(s.data_airHumidity)
                FROM sensor_zatu AS s, land AS l
                WHERE ST_Intersects(s.geometry, l.geometry) 
                GROUP BY devEUI, land_id WINDOW 2minutes 1minutes
                HAVING devEUI=1
            """,
            """
                SELECT s.devEUI, AVG(s.data_temperature), AVG(s.data_airHumidity)
                FROM sensor_zatu AS s
                WHERE s.devEUI=434e535301e36241
                GROUP BY devEUI WINDOW 2minutes 1minutes
            """
        ]

expected_parse_results= [
        {
            "columns": "s.devEUI, l.land_id, AVG(s.data_temperature), "
            "AVG(s.data_airHumidity)",
            "from": "sensor_zatu AS s, land AS l",
            "group": "devEUI, land_id",
            "where": "WHERE ST_Intersects(s.geometry, l.geometry) AND s.devEUI=HE363",
            "window": "2minutes 1minutes",
        },
        {
            "columns": "s.devEUI, l.land_id, s.data_temperature, "
            "s.data_airHumidity",
            "from": "sensor_zatu AS s, land AS l",
            "where": "WHERE ST_Intersects(s.geometry, l.geometry) AND s.devEUI=HE363",
        },
        {
            "columns": "s.devEUI, l.land_id, AVG(s.data_temperature), "
            "AVG(s.data_airHumidity)",
            "from": "sensor_zatu AS s, land AS l",
            "group": "devEUI, land_id",
            "where": "WHERE ST_Intersects(s.geometry, l.geometry)",
            "window": "2minutes 1minutes",
            "having": "devEUI=1",
        },
        {
            "columns": "s.devEUI, AVG(s.data_temperature), AVG(s.data_airHumidity)",
            "from": "sensor_zatu AS s",
            "group": "devEUI",
            "where": "WHERE s.devEUI=434e535301e36241",
            "window": "2minutes 1minutes",
        },
]

expected_query_trees=[
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


def test_parse_result():

    for query, parse_result in zip(queries, expected_parse_results):
        parser = Parser(query)
        parser.scrap()
        assert parser.parse_result == parse_result

def test_query_result():

    for query, expected_query_tree in zip(queries, expected_query_trees):
        parser = Parser(query)
        parser.scrap()
        assert parser.build_query_tree() == expected_query_tree


def test_where_parser():

    test_case= "WHERE ST_Intersects(s.geometry, l.geometry) AND s.devEUI=HE363"
    expected_output = {"join":[("ST_Intersects",("geometry","s"),("geometry","l"))],"filter":[("=",("devEUI","s"),("HE363",))]}

    assert where_parser(test_case) == expected_output

    test_case= "WHERE ST_Intersects(s.geometry, l.geometry)"
    expected_output={"join":[("ST_Intersects",("geometry","s"),("geometry","l"))]}
    
    assert where_parser(test_case) == expected_output
