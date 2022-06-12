from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
from data_formatter import *

spark_conf = SparkConf().setMaster("local").setAppName("app")\
    .set('spark.jars.packages', 'org.apache.spark:spark-avro_2.12:3.2.1')\
    .set('spark.jars', '/Users/pietro/Desktop/BDM/Project/DataImporta/P2/development/utilities/postgresql-42.2.25.jre7.jar')
    

sc = SparkContext(conf=spark_conf)
spark = SparkSession(sc)

# hdfs_path = 'hdfs://localhost:9000/persistent/'
hdfs_path =''

# DATA FOR TBOX GRAPH
def tbox_peru(path_to_avro=True, path_to_metadata=True):
    
    if path_to_avro:
        path_to_avro = hdfs_path+'/Users/pietro/Desktop/BDM/Project/DataImporta/P2/development/test_data/version0.avro'
    if path_to_metadata:
        path_to_metadata =  hdfs_path+'/Users/pietro/Desktop/BDM/Project/Data.nosync/peru/metadata_copia/'
    df = spark.read.format('avro').load(path_to_avro)
    rdd=df.rdd
    
    
    # MERGING DATA AND METADATA
    meta_file_name = ["Partidas.txt", "Paises.txt", "Paises.txt", "MedTransporte.txt"]
    key_pos_in_meta =  [0,0,0,0]
    name_pos_in_meta = [1,1,1,1] # position of the columns in meta that contains the name we want to keep 
    key_name_in_data = ["PART_NANDI", "PAIS_ORIGE", "PAIS_ADQUI", "VIA_TRANSP"]
    keep_code = [True, False, False, False] # true if we want to keep the code of the variable in the final data
    new_col_name = ["custom_description", "country_of_origin", "country_of_arrival", "mean_of_transport"]
    key_as_int = [True, False, False, True]
    
    rdds = []
    rdds.append(rdd)
    n_merging = len(name_pos_in_meta)
    
    for i in range(0,n_merging):
        rdds.append(merging_data_meta(rdds[i], meta_file_name[i], key_pos_in_meta[i], name_pos_in_meta[i], key_name_in_data[i], keep_code[i], new_col_name[i], key_as_int[i], path_to_metadata, "peru")) 
        #rdds.append(rdd)
   
    
    # COMBINING COLUMNS
    source_col_names = [["DESC_COMER", "DESC_MATCO", "DESC_USOAP", "DESC_FOPRE", "DESC_OTROS"]]
    dest_col_name = ["commercial_description"]
    string = [True]
    operation = ["+"]
    
    for i in range(0, len(dest_col_name)):
        rdds.append(create_composed_columns(rdds[n_merging + i], source_col_names[i], dest_col_name[i], string[i], operation[i])) 
    n_combining = len(source_col_names)


    # CHANGING NAMES
    old_names = ["EMPR_TRANS", "DNOMBRE", "FOB_DOLPOL", "UNID_FIQTY"]
    new_names = ["shipper", "trader", "net_price", "weight"]
    rdds.append(rdds[n_merging + n_combining].map(lambda l: changeNames(l, old_names, new_names)))
  
    
    
    # SELECT THE COLUMNS TO BE KEPT
    to_be_kept = ["custom_description", "country_of_origin", "country_of_arrival", "mean_of_transport", "shipper", "trader", "net_price", "commercial_description", "weight"]
    rdds.append(rdds[n_merging + n_combining + 1].map(lambda l: keepOnly(l, to_be_kept)))
    return(rdds[n_merging + n_combining + 2])
    
def main(to_path):
    df = tbox_peru().toDF()
    return df.toPandas().to_csv()

main('/Users/pietro/Desktop/peru_tbox.csv')