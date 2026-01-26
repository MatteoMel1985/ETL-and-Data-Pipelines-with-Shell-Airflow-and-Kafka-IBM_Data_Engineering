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

Now we have to match the table in README:


| Parameter | Value |
| --------- | ----- |
| DAG id | `ETL_toll_data` |  
| Schedule |		Daily once | 
| default_args |	as you have defined in the previous step | 
| description | Apache Airflow Final Assignment |    


Example (ensure to fill this code with your name and email):

```python
default_args = {
    "owner": "Your name",
    "start_date": days_ago(0),
    "email": ["your@email.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
```

## 4) DAG definition
Per README:

- DAG id: `ETL_toll_data`
- schedule: daily once (commonly `timedelta(days=1)`)
- description: `Apache Airflow Final Assignment`

Example:

```python
dag = DAG(
    "ETL_toll_data",
    default_args=default_args,
    description="Apache Airflow Final Assignment",
    schedule_interval=timedelta(days=1),
)
```

---

<h1 align="center">Exercise 3: Create Python functions</h1>

The key difference vs the BashOperator version is:

- Instead of `curl`, `tar`, `cut`, `paste`, `tr`
- You write **Python functions** that do the same operations, then call them using **PythonOperator**.

### Set shared paths and URLs
The README gives:

- **Source URL**: the `tolldata.tgz` link
- **Destination**: `/home/project/airflow/dags/python_etl/staging`

Use constants near the top:

```python
SOURCE_URL = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"
STAGING_DIR = Path("/home/project/airflow/dags/python_etl/staging")
TGZ_PATH = STAGING_DIR / "tolldata.tgz"
```

---

## 1) download_dataset
Download the `.tgz` file into the staging directory.

Implementation idea:

- `requests.get(..., stream=True)`
- write bytes to `TGZ_PATH`

Example:

```python
def download_dataset():
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    with requests.get(SOURCE_URL, stream=True) as r:
        r.raise_for_status()
        with open(TGZ_PATH, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
```

Why streaming matters: it avoids loading the entire archive into memory.

---

## 2) untar_dataset
Untar `tolldata.tgz` into the same staging folder.

Example:

```python
def untar_dataset():
    with tarfile.open(TGZ_PATH, "r:gz") as tar:
        tar.extractall(path=STAGING_DIR)
```

After extraction, you should have files like:

- `vehicle-data.csv`
- `tollplaza-data.tsv`
- `payment-data.txt`
- (often also `fileformats.txt`, which describes each file)

---

## 3) extract_data_from_csv
Extract these fields from `vehicle-data.csv`:

- Rowid
- Timestamp
- Anonymized Vehicle number
- Vehicle type

In CSV terms, these are typically columns 1–4.

Suggested approach:

- read the input CSV using `csv.reader`
- write a new CSV (`csv_data.csv`) with only the required columns

Example:

```python
def extract_data_from_csv():
    input_path = STAGING_DIR / "vehicle-data.csv"
    output_path = STAGING_DIR / "csv_data.csv"

    with open(input_path, newline="", encoding="utf-8") as infile,          open(output_path, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Optional: if input has header, keep it consistent:
        # header = next(reader, None)

        for row in reader:
            writer.writerow(row[0:4])
```

---

## 4) extract_data_from_tsv
Extract these fields from `tollplaza-data.tsv`:

- Number of axles
- Tollplaza id
- Tollplaza code

In the Bash version, these are typically columns 5–7.  
In Python (0-based indexing), that corresponds to indices **4, 5, 6**.

Example:

```python
def extract_data_from_tsv():
    input_path = STAGING_DIR / "tollplaza-data.tsv"
    output_path = STAGING_DIR / "tsv_data.csv"

    with open(input_path, newline="", encoding="utf-8") as infile,          open(output_path, "w", newline="", encoding="utf-8") as outfile:

        for line in infile:
            parts = line.rstrip("\n").split("\t")
            outfile.write(",".join(parts[4:7]) + "\n")
```

Tip: you can also use `csv.reader(infile, delimiter="\t")` if you want a consistent style.

---

## 5) extract_data_from_fixed_width
Extract these fields from `payment-data.txt`:

- Type of Payment code
- Vehicle Code

This file is **fixed-width**, so you do not split by delimiter.  
Instead, you use character slices.

### How to find the correct slice positions
If you have `fileformats.txt`, it typically documents how the fixed-width fields are arranged. If not, inspect one line:

```python
with open(STAGING_DIR / "payment-data.txt", "r", encoding="utf-8") as f:
    sample = f.readline()
print(repr(sample))
print(len(sample))
```

In the Bash-based case study, the payment code and vehicle code were extracted using character ranges like `60-62` and `64-68` (1-based indexing).  
In Python slicing (0-based, end-exclusive), that becomes:

- `Type of Payment code`: `line[59:62]`
- `Vehicle Code`: `line[63:68]`

Example:

```python
def extract_data_from_fixed_width():
    input_path = STAGING_DIR / "payment-data.txt"
    output_path = STAGING_DIR / "fixed_width_data.csv"

    with open(input_path, "r", encoding="utf-8") as infile,          open(output_path, "w", newline="", encoding="utf-8") as outfile:

        writer = csv.writer(outfile)
        for line in infile:
            pay_code = line[59:62].strip()
            veh_code = line[63:68].strip()
            writer.writerow([pay_code, veh_code])
```

If your dataset’s spacing differs, adjust the slice positions accordingly (that’s why inspecting a sample line is useful).

---

## 6) consolidate_data
Create `extracted_data.csv` by combining:

- `csv_data.csv`
- `tsv_data.csv`
- `fixed_width_data.csv`

The key idea (same as `paste` in Bash) is **row-wise concatenation**:

- read one row from each file
- write one combined row into the output

Example:

```python
def consolidate_data():
    csv_path = STAGING_DIR / "csv_data.csv"
    tsv_path = STAGING_DIR / "tsv_data.csv"
    fixed_path = STAGING_DIR / "fixed_width_data.csv"
    out_path = STAGING_DIR / "extracted_data.csv"

    with open(csv_path, newline="", encoding="utf-8") as f1,          open(tsv_path, newline="", encoding="utf-8") as f2,          open(fixed_path, newline="", encoding="utf-8") as f3,          open(out_path, "w", newline="", encoding="utf-8") as out:

        r1 = csv.reader(f1)
        r2 = csv.reader(f2)
        r3 = csv.reader(f3)
        w = csv.writer(out)

        for a, b, c in zip(r1, r2, r3):
            w.writerow(a + b + c)
```

---

## 7) transform_data
Transform the `Vehicle type` field in `extracted_data.csv` into uppercase and write:

`/home/project/airflow/dags/python_etl/staging/transformed_data.csv`

Because `Vehicle type` is the 4th column overall in the consolidated output (Rowid, Timestamp, Vehicle number, Vehicle type), it will usually sit at index `3` in the combined row.

Example approach (index-based):

```python
def transform_data():
    input_path = STAGING_DIR / "extracted_data.csv"
    output_path = STAGING_DIR / "transformed_data.csv"

    with open(input_path, newline="", encoding="utf-8") as infile,          open(output_path, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        for row in reader:
            if len(row) >= 4:
                row[3] = row[3].upper()
            writer.writerow(row)
```

---

<h1 align="center">Exercise 4: Create tasks using PythonOperators and define pipeline</h1>

## 1) Create tasks
For each function above, create a `PythonOperator` task:

```python
download_task = PythonOperator(
    task_id="download_dataset",
    python_callable=download_dataset,
    dag=dag,
)

untar_task = PythonOperator(
    task_id="untar_dataset",
    python_callable=untar_dataset,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id="extract_data_from_csv",
    python_callable=extract_data_from_csv,
    dag=dag,
)

extract_tsv_task = PythonOperator(
    task_id="extract_data_from_tsv",
    python_callable=extract_data_from_tsv,
    dag=dag,
)

extract_fixed_task = PythonOperator(
    task_id="extract_data_from_fixed_width",
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

consolidate_task = PythonOperator(
    task_id="consolidate_data",
    python_callable=consolidate_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)
```

## 2) Define the pipeline (dependencies)
The README pipeline order is:

1. download_dataset
2. untar_dataset
3. extract_data_from_csv
4. extract_data_from_tsv
5. extract_data_from_fixed_width
6. consolidate_data
7. transform_data

So the simplest chain is:

```python
download_task >> untar_task >> extract_csv_task >> extract_tsv_task >> extract_fixed_task >> consolidate_task >> transform_task
```

**Optional improvement (not always required by graders):**  
Once untar is done, the three extraction tasks can run in parallel:

```python
download_task >> untar_task >> [extract_csv_task, extract_tsv_task, extract_fixed_task] >> consolidate_task >> transform_task
```

---

<h1 align="center">Exercise 5: Save, submit, and run DAG</h1>

## 1) Save your DAG
Ensure the file is saved as `ETL_toll_data.py`.

## 2) Submit the DAG
Set Airflow home, then copy the DAG into the DAGs folder.

```bash
export AIRFLOW_HOME=/home/project/airflow
sudo cp ETL_toll_data.py $AIRFLOW_HOME/dags
```

(Depending on your lab structure, some exercises copy into a specific subfolder; the key is that Airflow must see it under `$AIRFLOW_HOME/dags`.)

## 3) Verify the DAG is visible
```bash
airflow dags list | grep ETL_toll_data
```

## 4) Unpause + trigger (CLI)
```bash
airflow dags unpause ETL_toll_data
airflow dags trigger ETL_toll_data
```

## 5) Observe runs / tasks
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

---

## Notes on common pitfalls

- **TSV extraction indices:** the required fields are typically columns 5–7; in Python indexing that’s `[4:7]`.
- **Fixed-width slicing:** always confirm slice positions by inspecting a real line (or checking `fileformats.txt`) before hardcoding.
- **Headers:** some datasets include headers; if so, decide whether to keep or skip them consistently across the pipeline.
- **Paths:** keep all file paths inside `STAGING_DIR` so tasks can find each other’s outputs.
