


 create external table effectivecare(ProviderID int, HospitalName String, Address String,City String,State String,ZIPCode Int,County String,Phonenumber int,Condition String, MeasureID String,MeasureName String,Score Int, Sample Int, Footnote String, Measurestartdate String,MeasureEnddate String)
 row format delimited
 fields terminated by ','
 stored as textfile
 location '/data/effective_care_trimed.csv';
 
 create external table hospitals(ProviderID int, HospitalName String, Address String,City String,State String,ZIPCode Int,County String,Phonenumber int,HospitaType String,HospitalOwnership String,EmergencyServices String)
 row format delimited
 fields terminated by ','
 stored as textfile
 location '/data/Hospitals_trimed.csv';
 
   create external table surveyresponse(ProviderID int, HospitalName String, Address String,City String,State String,ZIPCode Int,County String,points1 String,points2 String,points3 String,points4 String,points5 String,points6 String,points7 String,points8 String,points9 String,points10 String,points11 String,points12 String,points13 String,points14 String,points15 String,points16 String,points17 String,points18 String,points19 String,points20 String,points21 String,points22 String,points23 String,points24 String,Basescore Int,Consistencyscore int)
 row format delimited
 fields terminated by ','
 stored as textfile
 location '/data/survey_responses_trimed.csv';
 
 show tables;
 