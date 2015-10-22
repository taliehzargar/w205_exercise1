from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import * 
sqlContext=SQLContext(sc)

lines=sc.textFile("file:///root/exercise1/w205_exercise1/loading_and_modeling/effective_care_clean_trimed.csv")
parts=lines.map(lambda l: l.split(','))
Procedure=parts.map(lambda p: (p[0],p[2],p[7],p[8]))
schemaString= 'ProviderID State MeasureID score'
fields=[StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema=StructType(fields)
schemaprocedure=sqlContext.createDataFrame(Procedure,schema)
schemaprocedure.registerTempTable('Procedure')


#lines=sc.textFile("file:///root/exercise1/w205_exercise1/loading_and_modeling/effective_care_trimed.csv")
#parts=lines.map(lambda l: l.split(','))
#Procedure=parts.map(lambda p: (p[0],p[9],p[10],p[11],p[12],p[14],p[15]))
#schemaString= 'ProviderID MeasureID Measurename score sample MeasureStartDate MeasureEndDate'
#fields=[StructField(field_name, StringType(), True) for field_name in schemaString.split()]
#schema=StructType(fields)
#schemaprocedure=sqlContext.createDataFrame(Procedure,schema)
#schemaprocedure.registerTempTable('Procedure')
#results2 = sqlContext.sql('SELECT * FROM Procedure')
#results2.show()

result6 = sqlContext.sql('SELECT AVG(score) AS A1,ProviderID FROM Procedure GROUP BY ProviderID ORDER BY A1 DESC')

