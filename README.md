![Skills_Network](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/images/image.png)  

<h1 align="center">Build ETL Data Pipelines with BashOperator using Apache Airflow</h1>

# ***Project Scenario***  

You are a data engineer at a data analytics consulting company. You have been assigned a project to decongest the national highways by analyzing the road traffic data from different toll plazas. Each highway is operated by a different toll operator with a different IT setup that uses different file formats. Your job is to collect data available in different formats and consolidate it into a single file.  

# ***Objectives***  

In this assignment, you will develop an Apache Airflow DAG that will:  

* Extract data from a csv file
* Extract data from a tsv file
* Extract data from a fixed-width file
* Transform the data
* Load the transformed data into the staging area

## ***Exercise 1: Set up the lab environment***  

1. Start Apache Airflow.
2. Open a terminal and create a directory structure for the staging area as follows:

`/home/project/airflow/dags/finalassignment/staging.`  

```bash
sudo mkdir -p /home/project/airflow/dags/finalassignment/staging
```

3. Execute the following commands to give appropriate permission to the directories.

```bash
sudo chmod -R 777 /home/project/airflow/dags/finalassignment
```

4. Download the data set from the source to the following destination using the `curl` command.

```bash
sudo curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o /home/project/airflow/dags/finalassignment/tolldata.tgz
```

## ***Exercise 2: Create imports, DAG argument, and definition***  

Please use the `BashOperator` for all tasks in this assignment.  

1. Create a new file named `ETL_toll_data.py` in `/home/project` directory and open it in the file editor.

2. Import all the packages you need to build the DAG.

3. Define the DAG arguments as per the following details in the `ETL_toll_data.py` file:

| Parameter | Value |
| --------- | ----- |
| owner | &lt;You may use any dummy name&gt; |  
| start_date |	today | 
| email | &lt;You may use any dummy email&gt; | 
| email_on_failure | True | 
| email_on_retry | True | 
| retries | 1 | 
| retry_delay | 5 minutes | 

*Take a screenshot* of the task code. Name the screenshot `dag_args.jpg`.  

4. Define the DAG in the `ETL_toll_data.py` file using the following details.

| Parameter | Value |
| --------- | ----- |
| DAG id | `ETL_toll_data` |  
| Schedule |		Daily once | 
| default_args |	As you have defined in the previous step | 
| description | Apache Airflow Final Assignment |   

*Take a screenshot* of the task code. Name the screenshot `dag_definition.jpg`.  
