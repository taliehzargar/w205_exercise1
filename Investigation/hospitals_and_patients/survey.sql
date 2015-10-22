results2 = sqlContext.sql('SELECT * FROM survey ORDER BY CAST(Basescore AS INT) DESC')

results2.show()