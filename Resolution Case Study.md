<h1 align="center">Exercise 1: Set up the lab environment</h1>  

Start Apache Airflow by clicking the purple botton on the screen, or by clicking on the left Skills Network Toolbox and select BIG DATA > Apache Airflow, then click on the Create button, and wait for it to get started. 
Proceed now by opening a new terminal by selecting Terminal on the Menu Bar, and selecting New Terminal (or, alternatively, pressing the key combination ``Ctrl`` + ``Shift`` + `` ` ``.  

To create a directory structure for the staging area (which will be in the path `/home/project/airflow/dags/finalassignment/staging`), launch the following string from the terminal.

```bash
sudo mkdir -p /home/project/airflow/dags/finalassignment/staging
```

Execute the following commands to give appropriate permission to the directories.  

```bash
sudo chmod -R 777 /home/project/airflow/dags/finalassignment
```

Finally, download the data set from the source to the following destination using the `curl` command.  

```bash
sudo curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o /home/project/airflow/dags/finalassignment/tolldata.tgz
```

<h1 align="center">Exercise 2: Create imports, DAG argument, and definition</h1>  

To create a file named `ETL_toll_data.py`, ensure that the path in the command prompt is `/home/project`, and launch the following string.  

```bash
touch ETL_toll_data.py
```

Alternatively, you can click on `File` on the Menu Ribbon, and select `New File...`, then, digit `ETL_toll_data.py`, then, once the following window is opened, ensuring that the file will be created in the path `/home/project`, and clicking on the lower-right button `Create File`.  

Point 2 and 3 of Exercise 2 requires us to import all the needed packages to build the DAG, and to define its arcument as per the details provided in the following table.  

| Parameter | Value |
| --------- | ----- |
| owner | &lt;You may use any dummy name&gt; |  
| start_date |	today | 
| email | &lt;You may use any dummy email&gt; | 
| email_on_failure | True | 
| email_on_retry | True | 
| retries | 1 | 
| retry_delay | 5 minutes |   

To do so, we must click on the Explorer Pane on the upper left, select `PROJECT`, then, double click on the file `ETL_toll_data.py` and wait for it to appear on the editor pane. 

![Screenshot 1](https://github.com/MatteoMel1985/Relational-Dataset-Images/blob/main/Airflow/Screenshot%201.png?raw=true) 

Once you see it, write the following code (which is technically identical to the import section provided in Hands-on Lab: Create a DAG for Apache Airflow with BashOperator from Module 3 of the course), and ensure to insert your email.  

```bash
# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
#defining DAG arguments

default_args = {
    'owner': 'Artifex Datorum',
    'start_date': days_ago(0),
    'email': ['write your  email here'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

```  

Once you are done, take a screenshot of the task and name it `dag_args.jpg`.

![dag_args.jpg`](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tasks/1dag_args.jpg?raw=true)  

Point 4 
