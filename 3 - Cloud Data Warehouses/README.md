# Sparkify's Cloud ETL
Song Sessions at Scale

### Sparkify Streaming

Sparkify is a startup streaming service experiencing quick growth. As users and usage increases, so does our desire to make 
the user experience better. To do this, we need a robust backend so that analysts can confidently use our data warehouse
to build insights into our users trends, that will ultimately be the base for future improvements.

### Moving to the cloud

As Sparkify continues to grow, so does our database needs. In order to scale without knowing exactly how much and how soon, we're movingour data needs to the cloud via AWS. More specifically, we'll be using the data warehouse RedShift.

### Relational Model for Better Analytics

To get accurate analytics, we need robust data. For this reason, we are using a relational data model. We built a star schema 
where the fact events are suported by artist, song, and user dimensions. This script takes raw `json` files that are found in S3, and places them in staging/raw tables in Redshift. Using SQL queries, they are then transformed into the final tables, living in our chosen schema. From here, our analytics team can access user data with the confidence in its accuracy.

### In this Repo and how to run
The following files are found in this repo:

*  `create_tables.py`
*  `etl.py`
*  `sql_queries.py`

ALL string queries can be found in the `sql_queries.py` file. They are imported into the other py files.to run you simply need to run `create_tables.py` to create the tables and `etl.py` to fill them. You WILL need to create a config file for this script to run, as it pulls from a filed saved as `dwh.cfg` that houses the following:

* CLUSTER
    * HOST
    * DB_NAME
    * DB_USER
    * DB_PASSWORD
    * DB_PORT
* IAM_ROLE
    * ARN
* S3
    * LOG_DATA
    * LOG_JSONPATH
    * SONG_DATA