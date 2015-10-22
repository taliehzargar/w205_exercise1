
result_best_avg_score_state = sqlContext.sql('SELECT AVG(score) AS A1,State FROM Procedure2 GROUP BY State ORDER BY A1 DESC')
result_best_avg_score_state.show()

result_highest_variability_in_score_state = sqlContext.sql('SELECT ABS(min(score)-max(score)) AS A1,State FROM Procedure GROUP BY State ORDER BY A1 ASC')
result_highest_variability_in_score_state.show()