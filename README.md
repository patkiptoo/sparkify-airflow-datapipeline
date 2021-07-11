# sparkify-airflow-datapipeline
Apache Airflow datapipeline to load redshift from s3 on an hourly schedule with client listening data for analysis


## Use Case
Sparkify is a music streaming service. Customer streaming data is trackked and stored in json format in s3. To better understand and   
server the customers, the data needs to be analyzed. This pipiline loads that data every hour for analysis to redshift star schema data warehouse.

## Requirements
1. Airflow setup
2. AWS access to s3 json logs
3. redshift cluster running on AWS
4. create tables using the create_tables.py script in redshift.
5. If airflow is running loaclly or outside AWS, then need to enable access on redshift to access from outside AWS.

## How to execute
1. Place the DAGs in the dag folder of airflow
2. Place the operators in the plugins operator folder
3. Reload the DAGs in airflow UI. It should show the DAG udac_example_dag
     the schedule is every top of the hour with no backfilling.
4. Add the needed connections in airflow > admin menu item
    a. s3 connection using amazon web serivces
    b. redshift connection
5. Turn it on.
6. Should run are top of the hour.


## Sample Run Graphs

The graph view showing the tasks and their dependencies.
![alt text](https://github.com/patkiptoo/sparkify-airflow-datapipeline/blob/main/grapgh-view.JPG)

A sample run output depicting relative runtime.
![alt text](https://github.com/patkiptoo/sparkify-airflow-datapipeline/blob/main/gnatt-execution-graph.JPG)
