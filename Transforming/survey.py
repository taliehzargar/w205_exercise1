from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import * 
sqlContext=SQLContext(sc)
lines=sc.textFile("file:///root/exercise1/w205_exercise1/loading_and_modeling/survey_responses_trimed.csv")
parts=lines.map(lambda l: l.split(','))
survey=parts.map(lambda p: (p[0],p[31],p[32]))
schemaString= 'ProviderID Basescore Consistencyscore'
fields=[StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema=StructType(fields)
schemasurvey=sqlContext.createDataFrame(survey,schema)
schemasurvey.registerTempTable('survey')
results = sqlContext.sql('SELECT * FROM survey')
results.show()