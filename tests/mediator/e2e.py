####################
#
# End to end test run
#
#

query = """
            SELECT s.devEUI, l.land_id, AVG(s.data_temperature), AVG(s.data_airHumidity)
            FROM sensor_zatu AS s, land AS l
            WHERE ST_Intersects(s.geometry, l.geometry) AND s.devEUI=434e535301e36241
            GROUP BY devEUI, land_id WINDOW 2minutes 1minutes
        """

import sys
sys.path.append('.')

# build syntax_tree
from mediator.query_parser import Parser
parser = Parser(query)
parser.scrap()
syntax_tree = parser.build_query_tree()

# write spark app
from mediator.spark_app import SparkAppWritter
spark_app = SparkAppWritter(syntax_tree).write()
for step in spark_app.print():
    print(step)
print(spark_app.get_tables())

# load global dataframes
from mediator.wrapper import Wrapper
import os
wrapper = Wrapper("schema1", "e2e test", spark_master=os.getenv("SPARK_MASTER","spark://127.0.0.1:7077"))
source_dataframes = wrapper.load_global_view(spark_app.get_tables())

# convert spark app plan to dag end df
end_df = spark_app.get_dataframe(source_dataframes)

#plan
print(end_df.explain(True))

# start query
end_df.writeStream.outputMode("update") \
.format("console") \
.option("truncate", "false") \
.start() \
.awaitTermination()   



