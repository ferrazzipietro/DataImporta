# DataImporta

This project implements the necessary pipeline to organize in persistent storage (landing zone) and perform analytics on open data about foreing trades of three Latin American countries (Chile, Brazil and Perú).

## Table of Contents
- [Requirements](#requirements)
- [Structure of data sources](#structure-of-data-sources)
    - [Brazil](#brazil)
    - [Perú](#peru)
    - [Chile](#chile)
- [Temporal Landing Zone](#temporal-landing-zone)
- [Data collector from source to HDFS](#data-collector-from-source-to-HDFS)
- [Persistent landing zone](#persistent-landing-zone)
- [Persistent loading](#persistent-loading)
- [Formatted Zone](#formatted-zone)
- [Data Formatter](#data-formatter)Views refreshing
- [Views refreshing](#views-refreshing)
- [Descriptive Analysis](#descriptive-analysis)
- [Machine Learning Model](#machine-learning-model)





## Requirements

- Hadoop (HDFS). Installation guide for IOS at https://towardsdatascience.com/installing-hadoop-on-a-mac-ec01c67b003c. Installation guide for Windows at https://www.evernote.com/shard/s322/sh/4b3d1b2b-9b1a-7711-370f-2e15e0bac160/7f6c202359cf6e96e422501937d2a8af.
- PostgreSQL
- R
- Apache Spark
- Python 3
- Java 1.8.0


## Structure of data source

The data and the metadata of interest has to be downloaded from the websites of the three countries and to be organized in folders following a strict schema:

- Brazil: Data/brazil/metadata will contain all the metadata for brazilian data
          Data/brazil/csv/imp will contain all the import data for brazilian data
          Data/brazil/csv/exp will contain all the export data for brazilian data
          
- Perú: the format of these files is DBF. The conversion can be performed using _dbf2csv.ipynb_.
        Data/peru/metadata ...
        Data/peru/csv/imp...
        Data/peru/csv/exp...
        
- Chile: this files are compressed. They need to be unzipped.
        Data/chile/metadata/imp will contain all the metadata for brazilian import data
        Data/chile/metadata/impwill contain all the metadata for brazilian export data
        Data/chile/txt/imp ...
        Data/chile/txt/imp ...
       
For this prototype, the data source is simpified in the sence that the data will be manually downloaded and and automatically imported.



## Temporal landing zone

Once data is imported into the "data source", we can bring it to the temporal landing zone implemented in HDFS. The files are stored as row data following the schema that has to be defined using _temporal_lz_schema_. This area rapresent a temporal zone where raw data is saved waiting to be persistently saved in memory in a more efficient way.



## Data collector from source to HDFS

Once the structure of the temporal zone is defined, the notebook that perform the loading of the data from source to HDFS is _data_collector_.



## Persistent landing zone

The next step is to  save the data persistently in HDFS. The files are stored following the schema that has to be defined using _persistent_lz_schema_.



## Persistent loading

To save efficiently the data, in order to be able to distribute the files, they are converted to avro format (horizontally fragmentated) and saved yearly.
Every time (week or month) that new data for the current year is published, it is saved in the temporal lz and then transferred to the persistent one. The new information is appended to the one already present for that specific year, creating a new version of the data and keeping in memory the old version. This process is performed trough _data_persistance_loader_.

For a proper data governance all the steps and the outcomes are logged in the _log_ file.



## Formatted Zone

The formatted zone is a DB in PostgreSQL were some materialized views contain usefull aggregations

To create the database, set up the materialized views and the indexes run from postgres:
% \i create_db.sql



## Data Formatter

The data of the three different countries is formatted in order to have the same structure. For semplicity, a test_set is given with data in avro format and metadata in the source format (i.e., the original one, csv or txt).

Ensure you have Java 1.8.0 (other JDKs may give issues)
Once the DB has been created it can be populated with data prepared and cleaned in pyspark.
Run the following command from the /development folder (IMPORTANT!):

% python3 data_formatter.py db_user db_password dataimporta temporal data_from_HDFS_True_False

db_user: name of user for postgres (without "")
db_password: password of user for postgres (without "")
data_from_HDFS_Ture_False: Set to True if the data comes from HDFS and set to False if the data comes from a folder in the local filesystem



## Views refreshing

At each upload (i.e., run of the data formatter), the sandbowes (i.e., the materialized views) have to be refreshed. It is done running the following command inside PostgreSQL

% \i refresh_materialized_views.sql



## Descriptive Analysis

For this stage there are two options:

Make the whole connection and do the graphs or use the exported Tableau file that we created for simplicity:

Option 1:

In this stage, it is necessary to connect PostgreSQL and Tableau. For that open Tableau Desktop and select Connect/To a Server/PostgreSQL.
Then add the following parameters:
Server: 127.0.0.1
Port: 5432 (unless PostgreSQL port is defined differently in your system)
Database: dataimporta
Authentication: Username and Password
Username: <Your PostgreSQL username>
Password: <Your PostgreSQL password>

Now the data is available and it's a matter to drag and drop items to show the graphs.

Option 2: 

For simplicity, we provide a Tableau Packaged Workbook file named "descriptive_analysis.twbx" that is ready to use. It contains an interactive dashboard with all the graphs that we
want to show and the data from the respective sandbox is already packed inside, so there is no need to establish a connection to PostgreSQL.
    
     

## Machine Learning Model

We build a model to forecast the price of transport on the following features: country of arrival, mean of transport, custom, unit price and net price of the goods.         
          
Instal packages in R with:
install.packages('remotes')
install.packages('RPostgres')
install.packages("sparklyr")

Note: posgres session has to be open
To train and save the model, from command line launch:

% Rscript --default-packages=DBI,RPostgres,RPostgreSQL,dplyr,sparklyr train_model_transp_price.R path_to_where_save_the_model


To do forecasts:

% Rscript --default-packages=sparklyr,RPostgres forecasts_model_transp_price.R path_to_model country mean_of_transport custom net_price net_price_unit     

path_to_model: between brackets
