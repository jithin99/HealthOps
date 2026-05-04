HealthOPs_Pipeline
Datasets Taken : MIMIC-III Clinical Database_Project_Dataset
I have 4 csv’s in this dataset :
•	ADMISSIONS (129 Patient Admissions Records)
•	PATIENTS (Contain Patient demographics)
•	LABEVENTS (Contain Vitals of Patient important for risk analysis)
•	D_LABITEMS (metadata of Lab events.csv)
The Process I have done to write logic (initial notebook) :
Admissions.csv :
-	Converted admit,dischtime to to_datetime
-	Created length of stay (dischtime-admit time)
-	Removed unwanted columns
-	Removed death time (as we have hosp_expiry flag and has many nulls)
-	Admission typer to upper 
-	Mapped the emergency flag as 0,1,2
Patients.csv :
-	Removed Unwanted Columns (less priority)
Labevents.csv :
-	Removed the unwanted columns (less priority)
-	Kept columns (patient_id,admission_id,test_id,test_value)
Labitems.csv :
-	Dropped less priority columns and kept item_id,label
Then  
-	Created new df named merge joining (labevents+labitems) inner_join on item_id
-	Removed all other labels other than glucose and creatine as we have many different type of labels so I sticked to glucose and creatine
-	Group by subject_id,hadm_id,label,avg(num) for one patient one record
-	Then created one long to short format agg_pivot
-	Df= Agg_pivot + patients(on subject_id , left)
-	Df + risk score
-	Df + risk category








Workflow Diagram
 
Fig : Workflow of the Project (Plan of notebook structure)


 
-	Faced some error while saving the save and accessing in next job 
-	So later as I am using python for logic I save using pyspark then I have write logic in pyspark or use to pandas which is slow and complicated 
-	Then stored data in Parquet format so that I can write logic in python 
-	So I have created volume healthcare and then I stored parquet files in it
-	Parquet (Instead of storing data row by row (like CSV),
Parquet stores data column by column)
-	I have created silver, gold layers
-	Data lake folder structure as Parquet need folder to store so I created gold and silver 
 
Fig : Connect the databricks to AD and created a repo containing notebook
-	After Runing the Job I have connected connection between AD and Databricks and then created a new repo HealthOPs and moves all my notebooks to that folder and pushed them to AD
VS CODE IMPLEMENTATION (.DAB)
-	I have created a databricks assert bundle with name Healthops using (databricks bundle init)
-	Then I have created folder notebooks and uploaded all my .ipynb file in it 
-	Then modified the databricks.yml adding task and dependency 
-	I have created the dependency according the workflow as in below image
-	 
-	Fig : healthops_pipeline Timeline
-	Then I have also added email notification in the yml file that send email when the job runs successfully or failed 
-	Then I have deployed my job from local to db (databricks bundle deploy)
-	Then I have runed the deployed file (databricks bundle run healthops_pipeline)
 
Fig :Azure Devops repo (I have done commit from vs code in to master branch)
