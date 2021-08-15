# airflow_sparkify

## TO-DO:

- [X] Attempted to deploy code into Airflow's local instance
- [ ] Ensured Airflow picks up DAG code
- [ ] Entered aws credentials into Airflow GUI variables
- [X] Created Redshift cluster (programmatically)
- [ ] Entered cluster endpoint as Airflow UI
- [ ] Tested DAG
- [ ] Doc-string functions
- [ ] Updated README
- [ ] Submitted project

This project is intended to analyze data for a hypothetical start-up called Sparkify. This music streaming start-up wants to analyze their song- and log-related data in a more efficient and risk-free way, by both using AWS Redshift and Apache Airflow.

The goal of this project is to build data warehousing capabilities for Sparkify, as well as data pipeline automization processes. More specifically, the song- and log-data (JSON) is stored in AWS S3 buckets, staged into AWS Redshift and subsquently the data is put into a star-schema within a PostgreSQL database within redshift. All these jobs are automated and orchastrated using Apache Airflow, an open source tool.

## Staging

![alt text](https://github.com/raul-bermejo/cloud_sparkify/blob/main/images/staging_tables.png)

The raw data produced by the Sparkify app is staged as raw data into Redshift. In other words, the schema from the raw data (see figure above) serves as a landing zone.

## Database schema

The implemented database schema can be seen in the ER diagram below.

![alt text](https://github.com/raul-bermejo/cloud_sparkify/blob/main/images/sparkify_erd_transparent.png)

The Entity Relationshiip Diagram (ERD) above is a Star Schema where the facts (or metrics) are represented by the songplays relation. The reason for this is to have the analysis of log and song data at the heart of the business. From the songplays relation one can observe the dimension of the sparkify business: users, artists, songs and time. Each of these relations represents a core business aspect of sparkify.

## Data Pipeline

# [fill in Airflow details]

The raw data stored in S3 buckets, is copied into the staging tables above in Redshift. Next, the data is routed from the staging tables into the Star-Schema for analytical query optimization. Note the ETL pipeline is implemented with a combination of Python for AWS and PostreSQL connection combined with the use of SQL queries.

## Dependencies

A tutorial to deploy Ariflow on a Windows machine locally via Docker can be found [here](https://dev.to/jfhbrook/how-to-run-airflow-on-windows-with-docker-2d01). The docker-compose.yml is included in this repo. Information about Airflow's Webserver container can be found [here](https://airflow.apache.org/docs/apache-airflow/stable/security/webserver.html#web-authentication). 

The following libraries need to be installed for the code to work: pandas, boto3, and airflow.

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install any of these libraries if needed.

```bash
pip install pandas
pip install boto3
pip install airflow
```


## Authors

The author of this repo is me, Raul Bermejo, as part of the Data Engineer program at Udacity.

## Usage

# [fill in Airflow details]


To load the data into Redshift using the code, the order of execution is:

(1) Create Redshift cluster using AWS (either progarmatically or through the consolole)
(2) Run create_tables.py to create the SQL tables  (both staging and star-schema)
(3) Run etl.py to load the data into Redshift
(4) Make sure you delete the Redshift cluster once the data is loaded

## Contributing
Pull requests are welcome. For other issues and/or changes, feel free to open an issue.
