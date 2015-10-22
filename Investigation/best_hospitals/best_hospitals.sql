
result_best_avg_score = sqlContext.sql('SELECT AVG(score) AS A1,ProviderID FROM Procedure GROUP BY ProviderID ORDER BY A1 DESC')
result_best_avg_score.show()

result_highest_variability_in_score = sqlContext.sql('SELECT min(score)-max(score) AS A1,ProviderID FROM Procedure GROUP BY ProviderID ORDER BY A1 DESC')
result_highest_variability_in_score.show()


