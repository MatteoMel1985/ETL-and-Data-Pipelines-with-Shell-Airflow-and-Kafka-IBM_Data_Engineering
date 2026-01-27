<h1 align="center">Exercise 1: Prepare the lab environment</h1>

## ***Start Apache Airflow***  

Start Apache Airflow by clicking the purple button on the screen, or by clicking on the left Skills Network Toolbox and selecting BIG DATA > Apache Airflow; then click on the Create button, and wait for it to get started. Proceed now by opening a new terminal by selecting Terminal on the Menu Bar, and selecting New Terminal (or, alternatively, pressing the key combination Ctrl + Shift + `).

## ***Create the staging directory***  

The assignment requires the staging directory:

`/home/project/airflow/dags/python_etl/staging`

To creat it, run the following code:

```bash
sudo mkdir -p /home/project/airflow/dags/python_etl/staging
```

## ***Fix permissions***  

To avoid permission errors when Airflow tasks write files:

```bash
sudo chmod -R 777 /home/project/airflow/dags/python_etl
```

---

<h1 align="center">Exercise 2: Add imports, define DAG arguments, and define DAG</h1>

## ***Create the DAG file***  

From `/home/project`, run the following code:

```bash
touch ETL_toll_data.py
```

Open `ETL_toll_data.py` in the editor.

## ***Imports***  

The README asks for **PythonOperator**, plus the tasks represented by the following table:  

| Parameter | Value |
| --------- | ----- |
| owner | &lt;You may use any dummy name&gt; |  
| start_date |	today | 
| email | &lt;You may use any dummy email&gt; | 
| retries | 1 | 
| retry_delay | 5 minutes |  


Which would require this import block:  

```python
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import requests
import tarfile
import csv
from pathlib import Path
```

## ***DAG arguments (default_args)***  

At this point, we will have to insert the DAG arguments by matching the [README](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/PythonOperator%20ETL/README.md) table.


| Parameter | Value |
| --------- | ----- |
| DAG id | `ETL_toll_data` |  
| Schedule |		Daily once | 
| default_args |	as you have defined in the previous step | 
| description | Apache Airflow Final Assignment |    


To do so, I wrote the following code (ensure to fill this code with your name and email):

```python
default_args = {
    "owner": "Your name",
    "start_date": days_ago(0),
    "email": ["your@email.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
```

## ***DAG definition***  

To define the table, we have been given the following table:  

| Parameter | Value |
| --------- | ----- |
| DAG id | `ETL_toll_data` |  
| Schedule |		Daily once | 
| default_args |	as you have defined in the previous step | 
| description | Apache Airflow Final Assignment |   

Which would generate the following code:

```python
dag = DAG(
    "ETL_toll_data",
    default_args=default_args,
    description="Apache Airflow Final Assignment",
    schedule_interval=timedelta(days=1),
)
```

<h1 align="center">Exercise 3: Create Python functions</h1>

### ***Set shared paths and URLs***  

README gives:

- **Source URL**: the `tolldata.tgz` link
- **Destination**: `/home/project/airflow/dags/python_etl/staging`

Which will lead us to define the path in the following fashion.

```python
# Define the path for the input and output files
source_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
destination_path = '/home/project/airflow/dags/python_etl/staging'
```

## ***`download_dataset`***  

Download the `.tgz` file into the staging directory by writing the same Python function that was taught in the course.

```python
# Function to download the dataset
def download_dataset():
    response = requests.get(source_url, stream=True)
    if response.status_code == 200:
        with open(f"{destination_path}/tolldata.tgz", 'wb') as f:
            f.write(response.raw.read())
    else:
        print("Failed to download the file")
```

## ***`untar_dataset`***  

Untar `tolldata.tgz` into the same staging folder.

```python
# Function to untar the dataset
def untar_dataset():
    with tarfile.open(f"{destination_path}/tolldata.tgz", "r:gz") as tar:
        tar.extractall(path=destination_path)
```

## ***`extract_data_from_csv`*** 

Extract the following fields from `vehicle-data.csv`:

- Rowid
- Timestamp
- Anonymized Vehicle number
- Vehicle type

Which are in the columns 1–4 of the CSV.

```python
# Function to extract data from CSV
def extract_data_from_csv():
    input_file = f"{destination_path}/vehicle-data.csv"
    output_file = f"{destination_path}/csv_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:

        writer = csv.writer(outfile)
        writer.writerow(['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type'])
        for line in infile:
            row = line.split(',')
            writer.writerow([row[0], row[1], row[2], row[3]])
```

## ***extract_data_from_tsv*** 

Extract the following fields from `tollplaza-data.tsv`:

- Number of axles
- Tollplaza id
- Tollplaza code

```python
# Function to extract data from TSV
def extract_data_from_tsv():
    input_file = f"{destination_path}/tollplaza-data.tsv"
    output_file = f"{destination_path}/tsv_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Number of axles', 'Tollplaza id', 'Tollplaza code'])
        for line in infile:
            row = line.split('\t')
            writer.writerow([row[0], row[1], row[2]])
```

## ***`extract_data_from_fixed_width`***  

Extract these fields from `payment-data.txt`:

- Type of Payment code
- Vehicle Code

This file is **fixed-width**, so you do not split by delimiter; instead, you use character slices (this has been thoroughly explained in the [Bash Operator Resolution Case Study](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Resolution%20Case%20Study.md)).

```python
# Function to extract data from fixed width file
def extract_data_from_fixed_width():
    input_file = f"{destination_path}/payment-data.txt"
    output_file = f"{destination_path}/fixed_width_data.csv"
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Type of Payment code', 'Vehicle Code'])
        for line in infile:
            writer.writerow([line[0:6].strip(), line[6:12].strip()])
```

## ***`consolidate_data`***  

Create `extracted_data.csv` by combining:

- `csv_data.csv`
- `tsv_data.csv`
- `fixed_width_data.csv`

```python
def consolidate_data():
    csv_file = f"{destination_path}/csv_data.csv"
    tsv_file = f"{destination_path}/tsv_data.csv"
    fixed_width_file = f"{destination_path}/fixed_width_data.csv"
    output_file = f"{destination_path}/extracted_data.csv"

    with open(csv_file, 'r') as csv_in, open(tsv_file, 'r') as tsv_in, open(fixed_width_file, 'r') as fixed_in, open(output_file, 'w') as out_file:
        csv_reader = csv.reader(csv_in)
        tsv_reader = csv.reader(tsv_in)
        fixed_reader = csv.reader(fixed_in)
        writer = csv.writer(out_file)

        writer.writerow(['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code'])
        next(csv_reader)
        next(tsv_reader)
        next(fixed_reader)

        for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_reader):
            writer.writerow(csv_row + tsv_row + fixed_row)

```

## ***`transform_data`***  

Transform the `Vehicle type` field in `extracted_data.csv` into uppercase and write:

`/home/project/airflow/dags/python_etl/staging/transformed_data.csv`


```python
def transform_data():
    input_file = f"{destination_path}/extracted_data.csv"
    output_file = f"{destination_path}/transformed_data.csv"

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()

        for row in reader:
            row['Vehicle type'] = row['Vehicle type'].upper()
            writer.writerow(row)

```

<h1 align="center">Exercise 4: Create a tasks using PythonOperators and define pipeline</h1>

## ***Create tasks***

For each function previously explained, and shown in the table below, now we proceed by creating a `PythonOperator` task for each:

| Task | Functionality |
| --------- | ----- |
| First task | `download_data` |
| Second task | `unzip_data` |  
| Third task |	`extract_data_from_csv` | 
| Fourth task | `extract_data_from_tsv` | 
| Fivth task | `extract_data_from_fixed_width` | 
| Sixth task | `consolidate_data` | 
| Seventh task | `transform_data` |   

```python
# Default arguments for the DAG
default_args = {
    'owner': 'Artifex Datorum',
    'start_date': days_ago(0),
    'email': ['your email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Define the tasks
download_task = PythonOperator(
    task_id='download_dataset',
    python_callable=download_dataset,
    dag=dag,
)

untar_task = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

extract_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_task = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)
```

## ***Define the pipeline (dependencies)***  

The README pipeline order is:

1. download_dataset
2. untar_dataset
3. extract_data_from_csv
4. extract_data_from_tsv
5. extract_data_from_fixed_width
6. consolidate_data
7. transform_data

So we can create the following chain which, once untar is done, the three extraction tasks are run in parallel:

```python
download_task >> untar_task >> [extract_csv_task, extract_tsv_task, extract_fixed_width_task] >> consolidate_task >> transform_task
```

<h1 align="center">Exercise 5: Save, submit, and run DAG</h1>

## ***Save your DAG*** 

Ensure the file is saved as `ETL_toll_data.py` and save on your EDI.

## ***Submit the DAG***  

### ***Note: All the commands are written for the CLI. For the Airflow version, follow the same procedure explained in [Bash Operator Resolution Case Study](https://github.com/MatteoMel1985/ETL-and-Data-Pipelines-with-Shell-Airflow-and-Kafka-IBM_Data_Engineering/blob/main/Resolution%20Case%20Study.md)*** 
Set Airflow home, then copy the DAG into the DAGs folder.

```bash
export AIRFLOW_HOME=/home/project/airflow
sudo cp ETL_toll_data.py $AIRFLOW_HOME/dags
```

## ***Verify the DAG is visible***  

```bash
airflow dags list | grep ETL_toll_data
```

## ***Unpause + trigger (CLI)***
```bash
airflow dags unpause ETL_toll_data
airflow dags trigger ETL_toll_data
```

## ***Observe runs / tasks***

List runs:

```bash
airflow dags list-runs -d ETL_toll_data
```

List tasks:

```bash
airflow tasks list ETL_toll_data
```

In the Airflow UI, you can also click the DAG, unpause it, then trigger it and inspect each task log to confirm outputs were created under:

`/home/project/airflow/dags/python_etl/staging`
