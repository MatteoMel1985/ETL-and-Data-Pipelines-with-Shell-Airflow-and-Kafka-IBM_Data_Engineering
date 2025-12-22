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


