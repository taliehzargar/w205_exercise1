results2 = sqlContext.sql('SELECT * FROM survey ORDER BY CAST(Basescore AS INT) DESC')

results2.show()

lines=sc.textFile("file:///root/exercise1/w205_exercise1/loading_and_modeling/effective_care_trimed_clean2.csv")
parts=lines.map(lambda l: l.split(','))
Procedure=parts.map(lambda p: (p[0],p[7],p[8]))
schemaString= 'ProviderID MeasureID score'
fields=[StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema=StructType(fields)
schemaprocedure=sqlContext.createDataFrame(Procedure,schema)
schemaprocedure.registerTempTable('Procedure')

result6 = sqlContext.sql('SELECT AVG(score) AS A1,ProviderID FROM Procedure GROUP BY ProviderID ORDER BY A1 DESC')

