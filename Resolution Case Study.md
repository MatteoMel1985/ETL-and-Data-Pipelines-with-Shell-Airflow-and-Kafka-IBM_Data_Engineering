<h1 align="center">Exercise 1: Set up the lab environment</h1>  

Start Apache Airflow by clicking the purple button on the screen, or by clicking on the left Skills Network Toolbox and selecting `BIG DATA` > `Apache Airflow`; then click on the `Create` button, and wait for it to get started. 
Proceed now by opening a new terminal by selecting Terminal on the Menu Bar, and selecting New Terminal (or, alternatively, pressing the key combination ``Ctrl`` + ``Shift`` + `` ` ``).  

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

Point 4 of the exercise require us to define the DAG in the `ETL_toll_data.py` file using the following details.

| Parameter | Value |
| --------- | ----- |
| DAG id | `ETL_toll_data` |  
| Schedule |		Daily once | 
| default_args |	As you have defined in the previous step | 
| description | Apache Airflow Final Assignment |   

Also this second task is modelled on the previous lab. Following is the code I wrote.  

```bash

# defining the DAG

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)
```

Finally, we can take a screenshot of this part of the script and save it as `dag_definition.jpg`.  

![dag_definition.jpg](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tasks/2dag_definition.jpg?raw=true)  


<h1 align="center">Exercise 3: Create the tasks using BashOperator</h1>  

The first task of Exercise 3 requires us to create a task named `unzip_data` to unzip data. Use the data downloaded in the first part of this assignment in `Set up the lab environment` and uncompress it into the destination directory using tar.  

To do so, I added the following section in `ETL_toll_data.py`.  

```bash
# define the tasks
# define the first task (unzipping data)
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag,
)
```

To complete the task, we can take a screenshot of this part of the code and save it as `unzip_data.jpg`.  

![unzip_data.jpg](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tasks/3unzip_data.jpg?raw=true)  

Proceeding with the exercise, we are now requested to create a task named `extract_data_from_csv` to extract the fields `Rowid`, `Timestamp`, `Anonymized Vehicle number`, and `Vehicle type` from the `vehicle-data.csv` file and save them into a file named `csv_data.csv`.  

Before sharing the code I wrote, it is worth analysing both files [fileformats.txt](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tolldata/fileformats.txt), sharing the explanations of how the information was stored in each file, and [vehicle-data.csv](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tolldata/vehicle-data.csv), containing the data we must extract.  

In `fileformats.txt`, we read the following:

```txt
vehicle-data.csv is a comma-separated values file.
It has the below 6 fields

Rowid  - This uniquely identifies each row. This is consistent across all the three files.
Timestamp - What time did the vehicle pass through the toll gate.
Anonymized Vehicle number - Anonymized registration number of the vehicle 
Vehicle type - Type of the vehicle
Number of axles - Number of axles of the vehicle
Vehicle code - Category of the vehicle as per the toll plaza.
```

Hence, `Rowid`, `Timestamp`, `Anonymized Vehicle number`, and `Vehicle type` are columns 1, 2, 3, and 4 of `vehicle-data.csv`. This will be specified in the bash command of the code `cut -d"," -f1-4`.  

```bash
# define the second task (extracting from csv)
extract_data_from_csv = BashOperator(
    task_id= 'extract_data_from_csv',
    bash_command= 'cut -d"," -f1-4 < /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag= dag,
)
```

Finally, we can take a screenshot of this part of the script and save it as `extract_data_from_csv.jpg`.  

![extract_data_from_csv.jpg](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tasks/4extract_data_from_csv.jpg?raw=true)  

The following request is substantially identical, except for the file format which, this time, is `.tsv`. Indeed, we are now required to create a task named `extract_data_from_tsv` to extract the fields `Number of axles`, `Tollplaza id`, and `Tollplaza code` from the `tollplaza-data.tsv` file and save it into a file named `tsv_data.csv`.  

By reading [fileformats.txt](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tolldata/fileformats.txt), we see that:

```txt
tollplaza-data.tsv:
tollplaza-data.tsv is a tab-separated values file.
It has the below 7 fields

Rowid  - This uniquely identifies each row. This is consistent across all the three files.
Timestamp - What time did the vehicle pass through the toll gate.
Anonymized Vehicle number - Anonymized registration number of the vehicle 
Vehicle type - Type of the vehicle
Number of axles - Number of axles of the vehicle
Tollplaza id - Id of the toll plaza
Tollplaza code - Tollplaza accounting code.
```

Hence, we are expected to extract the data from columns 5, 6, and 7. Since  the format `.tar` differs from `.csv` as the information is not comma separated, we don't need to insert the delimiter `-d","` in the bash command.  

```bash
# define the third task (extracting from tsv)
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 < /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)
```

As we wrote this section of the code, we can take a screenshot of it and save it as `extract_data_from_tsv.jpg`.  

![extract_data_from_tsv.jpg](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tasks/5extract_data_from_tsv.jpg?raw=true)  

It is worth noting that the `.txt` format works quite differently. We can see it now as we are called to create a task named `extract_data_from_fixed_width` to extract the fields `Type of Payment code`, and `Vehicle Code` from the fixed width file `payment-data.txt` and save it into a file named `fixed_width_data.csv`.  

Interestingly, what `fileformats.txt`say about [payment-data.txt](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tolldata/payment-data.txt#L1) could be quite deceiving:

```txt
payment-data.txt is a fixed width file. Each field occupies a fixed number of characters.

It has the below 7 fields

Rowid  - This uniquely identifies each row. This is consistent across all the three files.
Timestamp - What time did the vehicle pass through the toll gate.
Anonymized Vehicle number - Anonymized registration number of the vehicle 
Tollplaza id - Id of the toll plaza
Tollplaza code - Tollplaza accounting code.
Type of Payment code - Code to indicate the type of payment. Example : Prepaid, Cash.
Vehicle Code -  Category of the vehicle as per the toll plaza.
```

Indeed, we may be led to think that we are requested to extract the data from columns 6 and 7, but `.txt` files are not shaped in columns.  
Interestingly, though, we can observe that in [payment-data.txt](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tolldata/payment-data.txt#L1), each piece of information has the same length of characters. Therefore, by counting the characters of each line, we can determine where each piece of data has been stored. By carefully analysing the file, we can determine that `Type of Payment code` is scripted between the 59th and the 61st character of each line, whereas the 62nd character is a space that separates it from the next field, which is the `Vehicle Code`, comprehended between the 63rd and the 67th character. Thus, we can write this section of the code as follows.  

```bash
# define the fourth task (extracting from txt)
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c60-62,64-68 < /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)
```

Then, take a screenshot of it and save it as `extract_data_from_fixed_width.jpg`. 

![extract_data_from_fixed_width.jpg](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tasks/6extract_data_from_fixed_width.jpg?raw=true)  

Following, we are requested to create a task named `consolidate_data` to consolidate data extracted from previous tasks. This task should create a single csv file named `extracted_data.csv` by combining data from the following files:  

* `csv_data.csv`  
* `tsv_data.csv`
* `fixed_width_data.csv`

The final csv file should use the fields in the order given below:  

* `Rowid`
* `Timestamp Anonymized Vehicle number`
* `Vehicle type`
* `Number of axles`
* `Tollplaza id`
* `Tollplaza code`
* `Type of Payment code`, and
* `Vehicle Code`

At this point, it would be quite important to pay attention to the hint given in the instructions, which reads: *Use the bash `paste` command that merges the columns of the files passed as a command-line parameter and sends the output to a new file specified. You can use the command `man paste` to explore more. Example: `paste file1 file2 > newfile`*.  

Hence, I wrote the following code, which specified the path of each file of origin, and consolidates them in the file `extracted_data.csv` by using the paste command. The delimiter command `-d` tells the computer: *"Instead of using a tab, use a comma to separate the data from each file,"* given that our destination file is a `.csv`.

```bash
# define the fifth task (consolidating the data)
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=(
        "paste -d, "
        "/home/project/airflow/dags/finalassignment/csv_data.csv "
        "/home/project/airflow/dags/finalassignment/tsv_data.csv "
        "/home/project/airflow/dags/finalassignment/fixed_width_data.csv "
        "> /home/project/airflow/dags/finalassignment/extracted_data.csv"
    ),
    dag=dag,
)
```

As requested, we can now take a screenshot of this portion of the code and save it as `consolidate_data.jpg`. 

![consolidate_data.jpg](https://raw.githubusercontent.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/5a59591311a02329597a497644280f6a2c834202/Tasks/7consolidate_data.jpg)  

Continuing with the test, we are now required to create a task named `transform_data` to transform the `vehicle_type` field in `extracted_data.csv` into capital letters and save it into a file named `transformed_data.csv` in the staging directory. Interestingly, the hint provided reads *"You can use the `tr` command within the BashOperator in Airflow."* Technically, the instructions say it all. The following is the code I wrote.  

```bash
# define the sixth task (transform)
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv',
    dag=dag,
)
```

Once done, we can save the screenshot as `transform.jpg`.  

![transform.jpg](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tasks/8transform.jpg?raw=true)  

Finally, the last task of the exercise requires us to define the task of the pipeline according to the table provided.  

| Task | Functionality |
| --------- | ----- |
| First task | `unzip_data` |  
| Second task |	`extract_data_from_csv` | 
| Third task | `extract_data_from_tsv` | 
| Fourth task | `extract_data_from_fixed_width` | 
| Fifth task | `consolidate_data` | 
| Sixth task | `transform_data` |    

The following line is a perfect fit for Airflow.  

```bash
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
```

After taking a screenshot of this final part, we can save it as `task_pipeline.jpg`.  

![task_pipeline.jpg](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tasks/9task_pipeline.jpg?raw=true)  

<h1 align="center">Exercise 4: Getting the DAG operational</h1>  

To submit the DAG, run these commands from the terminal. 

```bash
export AIRFLOW_HOME=/home/project/airflow
```

* `export` creates an environment variable in your current terminal session.
* `AIRFLOW_HOME = /home/project/airflow` uses `AIRFLOW_HOME` to know where its entire working directory lives.

Finally, the following command to submit is:  

```bash
sudo cp ETL_toll_data.py $AIRFLOW_HOME/dags/finalassignment/staging
```

* `sudo` stands for "Super User Do" and runs the command with administrator privileges.
* `cp ETL_toll_data.py  $AIRFLOW_HOME/dags/finalassignment/staging` copies the file to the directory, so that Apache Airflow can read it.

Finally, to verify if the DAG has been successfully submitted, launch the following command from the terminal.  

```bash
airflow dags list | grep ETL_toll_data
```

The command `grep` will scan line by line the DAGs in Apache Airflow and, among them, will search `ETL_toll_data`.  If the DAG submission was successful, the requested screenshot `submit_dag.jpg` will appear as follow. 

![submit_dag.jpg](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Tasks/10submit_dag.jpg?raw=true)  


