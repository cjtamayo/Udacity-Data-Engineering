# Sparkify's Data Lake
Song Data Unstructured

### Sparkify Streaming

Sparkify is a startup streaming service experiencing quick growth. As users and usage increases, so does our desire to make 
the user experience better. To do this, we need a robust backend so that analysts can confidently use our data 
to build insights into our users trends, that will ultimately be the base for future improvements.

### Moving by the lake

As Sparkify continues to grow, so does our database needs. Without being tied to the rigid structure of a data warehouse or dimensional architecture,
we are building a lake format. Files will be read in via S3 and Apache Spark will be used to ingest and wrangle the data. From here, it will be saved
into our S3 buckets for analysts to grab and use for insights.

Despite this being overall a Data Lake architecture, this current group of tables does follow a general star schema for analytics to use and parse.
However, this also allows for more flexibility in the future by not making future tables conform to a warehouse architecture.

### In this Repo and how to run
The repo essentially has one file. A reference is made to `dl.cfg` to grab AWS credentials, but the work is done all via python. Simply run the 
`etl.py` file to grab and build the lake. 
