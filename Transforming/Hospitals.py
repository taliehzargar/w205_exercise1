from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import * 
sqlContext=SQLContext(sc)
lines=sc.textFile("file:///root/exercise1/w205_exercise1/loading_and_modeling/Hospitals_trimed.csv")
parts=lines.map(lambda l: l.split(','))
Hospitals=parts.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10]))
schemaString= 'ProviderID HospitalName Address City State ZIPCode CountyName PhoneNumber Hospital_type Hospital_ownership Emergency_Services'
fields=[StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema=StructType(fields)
schemahospitals=sqlContext.createDataFrame(Hospitals,schema)
schemahospitals.registerTempTable('Hospitals')
results3 = sqlContext.sql('SELECT * FROM Hospitals')
results3.show()