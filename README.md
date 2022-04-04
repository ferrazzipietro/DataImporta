# DataImporta

This project implements the necessary pipeline to organize in persistent storage (landing zone) open data about foreing trades of three Latin American countries (Chile, Brazil and Perú).

**Requirements**

Hadoop (HDFS). 
Installation guide for IOS at https://towardsdatascience.com/installing-hadoop-on-a-mac-ec01c67b003c.
Installation guide for Windows at https://www.evernote.com/shard/s322/sh/4b3d1b2b-9b1a-7711-370f-2e15e0bac160/7f6c202359cf6e96e422501937d2a8af.

**Structure of data source**

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


**Temporal landing zone**

Once data is imported into the "data source", we can bring it to the temporal landing zone implemented in HDFS. The files are stored as row data following the schema that has to be defined using _temporal_lz_schema_. This area rapresent a temporal zone where raw data is saved waiting to be persistently saved in memory in a more efficient way.


**Data collector from source to HDFS**

Once the structure of the temporal zone is defined, the notebook that perform the loading of the data from source to HDFS is _data_collector_.


**Persistent landing zone**

The next step is to finally save the data persistently in HDFS. The files are stored following the schema that has to be defined using _persistent_lz_schema_.

**Persistent loading**

To save efficiently the data, in order to be able to distribute the files, they are converted to avro format (horizontally fragmentated) and saved yearly.
Every time (week or month) that new data for the current year is published, it is saved in the temporal lz and then transferred to the persistent one. The new information is appended to the one already present for that specific year, creating a new version of the data and keeping in memory the old version. This process is performed trough _data_persistance_loader_.

For a proper data governance all the steps and the outcomes are logged in the _log_ file.

