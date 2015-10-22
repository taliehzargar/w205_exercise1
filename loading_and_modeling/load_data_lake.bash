cd exercise1

git clone https://github.com/taliehzargar/w205_exercise1.git

cd w205_exercise1/loading_and_modeling

tail -n +2 survey_responses.csv > survey_responses_trimed.csv

tail -n +2 survey_responses_NAremoved.csv > survey_responses_clean_trimed.csv

tail -n +2 readmissions.csv > readmisions_trimed.csv

tail -n +2 effective_care.csv > effective_care_trimed.csv

tail -n +2 effective_care_clean2.csv > effective_care_clean_trimed.csv

tail -n +2 Hospitals.csv > Hospitals_trimed.csv

hadoop dfs -mkdir data

cd exercise1/w205_exercise1/ loading_and_modeling
 
hadoop dfs -put survey_responses_trimed.csv data

hadoop dfs -put readmisions_trimed.csv data
 
hadoop dfs -put effective_care_trimed.csv data
 
hadoop dfs -put Hospitals_trimed.csv data

hadoop dfs -ls data

