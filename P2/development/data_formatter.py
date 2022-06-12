#import os  
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.2.1 pyspark-shell'    

from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import Row
import re
spark_conf = SparkConf().setMaster("local").setAppName("app")\
    .set('spark.jars.packages', 'org.apache.spark:spark-avro_2.12:3.2.1')\
    .set('spark.jars', '/Users/pietro/Desktop/BDM/Project/DataImporta/P2/development/utilities/postgresql-42.2.25.jre7.jar')
    

sc = SparkContext(conf=spark_conf)
spark = SparkSession(sc)

#spark = SparkSession.builder.getOrCreate()

#------------------------------------------------------
# FUNCTIONS
#------------------------------------------------------
# delete one column in a row
def delete(row, col_name):
    d = row.asDict()
    d.pop(col_name)
    return Row(**d)

# function add one key to a row
def insert(row, new_col_name, value):
    d = row.asDict()
    d[new_col_name] = str(value).rstrip()
    return Row(**d)
    
# get 1 if int, otherwise length of list
def lenght(val):
    if isinstance(val, int):
        return 1
    else:
        return len(val)

# update one field in row
def updateRow(row, field_to_update, value):
    d = row.asDict()
    d[field_to_update] = value 
    return Row(**d)

# change name of a field in a row
def changeNames(row, old_names, new_names):
    for i in range(0, len(old_names)):
        row = insert(row, new_names[i], row[old_names[i]])
        row = delete(row, old_names[i])
    return row

# keep only some fields from a row
def keepOnly(row, fields_to_be_kept):
    new_row = Row(var=True)
    for field in fields_to_be_kept:
        new_row = insert(new_row, field, row[field])
    new_row = delete(new_row, "var")
    return new_row  

def merging_data_meta (rdd, meta_file_name, key_pos_in_meta, name_pos_in_meta, key_name_in_data, keep_code, new_col_name, key_as_int, path_to_metadata, country):
    
    # METADATA
    if country == "peru":
        # if the first element starts with "C" or "t", it has to be dropped (it is the header)
        meta_split = sc.textFile(path_to_metadata + meta_file_name).filter(lambda l: not l.startswith(tuple(["C","t"]))).map(lambda l: tuple(l.split("\t")))
        #print(meta_split.take(4))
        #print(meta_split.take(2)) 
    
    elif country=="brasil":
        if meta_file_name == "NCM.csv":
            delim = ";"
            meta_split = sc.textFile(path_to_metadata + meta_file_name).filter(lambda l: not l.startswith("C")).map(lambda l: tuple(tuple(l.split("\n"))[0].split(delim)))
        else:
            delim="\",\""
            meta_split = sc.textFile(path_to_metadata + meta_file_name).filter(lambda l: not l.startswith("\"C")).map(lambda l: tuple(tuple(l.split("\n"))[0][1:-1].split(delim)))
    else:
        print('--- ERROR: wrong country name --')
    # prepare metadata for the join
    if lenght(key_pos_in_meta) == 2: # the key is the combination of 2 columns
        meta_ready_for_join0 = meta_split.map(lambda l: ( l[key_pos_in_meta[0]].strip() + l[key_pos_in_meta[1]].strip(), l[name_pos_in_meta].rstrip())).distinct()
    elif (key_as_int):
        meta_ready_for_join0 = meta_split.map(lambda l: ( int(l[key_pos_in_meta].strip()), l[name_pos_in_meta].rstrip())).distinct()
    else:
        meta_ready_for_join0 = meta_split.map(lambda l: ( l[key_pos_in_meta].strip(), l[name_pos_in_meta].rstrip())).distinct() 
    
    # if there are repetiotions of the key with typos on the attr. name that should instead be always the same
    meta_ready_for_join = meta_ready_for_join0.groupByKey().mapValues(list).map(lambda l: (l[0],l[1][0]))

    # DATA
    # prepare data for the join:
    if (key_as_int):
        # if there are nan, we set them to 9999999999
        data_ready_for_join = rdd.map(lambda l: updateRow(l, key_name_in_data, "9999999999") if l[key_name_in_data]=='nan' else l).map(lambda l: (int( l[key_name_in_data].split(".")[0]) , l) )
        # print(data_ready_for_join.take(5))
    else:
        data_ready_for_join = rdd.map(lambda l: ( l[key_name_in_data], l))
   
    # JOIN
    join = data_ready_for_join.leftOuterJoin(meta_ready_for_join)
    
    if (not keep_code): # remove the column with the code
        merged = join.map(lambda l:  (delete(l[1][0], key_name_in_data), l[1][1]) )
        #print(merged.take(1))

    else:
        merged = join.map(lambda l:  (l[1][0], l[1][1]) )
    
    complete = merged.map(lambda l:  insert(l[0], new_col_name, l[1]) )
    
    return complete

def create_composed_columns(rdd, source_col_names, dest_col_name, string = False, operation = "+"):
    
    # "+" (addition for numbers or concatenating strings)
    # "/" (division for numberes)
    
    if operation == "+":
        if string:
            return rdd.map(lambda l: insert(l, dest_col_name, ' '.join(l[s].strip() for s in source_col_names if l[s])))
        return rdd.map(lambda l: insert(l, dest_col_name, int(float(l[source_col_names[0]].replace(",","."))) + int(float(l[source_col_names[1]].replace(",","."))) ))
    if operation == "/":
        return rdd.map(lambda l: insert(l, dest_col_name, int(float(l[source_col_names[0]].strip())) / (int(float(l[source_col_names[1]].strip())) + 0.000001)))
#------------------------------------------------------

#------------------------------------------------------
# PERU PIPELINE (TEMPORARY)
#------------------------------------------------------
def main_peru(path_to_avro=True, path_to_metadata=True):
    
    if path_to_avro:
        path_to_avro = "/Users/pietro/Desktop/BDM/Project/DataImporta/P2/development/test_data/version0.avro"
    if path_to_metadata:
        path_to_metadata = '/Users/pietro/Desktop/BDM/Project/Data.nosync/peru/metadata_copia/'
    df = spark.read.format('avro').load(path_to_avro)
    rdd=df.rdd
    
    
    # MERGING DATA AND METADATA
    meta_file_name = ["Partidas.txt", "Paises.txt", "Paises.txt", "Puertos.txt", "MedTransporte.txt", "Agente.txt", "Bancos.txt", "RecintAduaner.txt", "EstMercancia.txt"]
    key_pos_in_meta =  [0,0,0,[0,2],0,0,0,0,0]
    name_pos_in_meta = [1,1,1,  3,  1,1,1,1,1] # position of the columns in meta that contains the name we want to keep 
    key_name_in_data = ["PART_NANDI", "PAIS_ORIGE", "PAIS_ADQUI", "PUER_EMBAR", "VIA_TRANSP", "CODI_AGENT", "BANC_CANCE", "CODI_ALMA", "SEST_MERCA"]
    keep_code = [True, False, False, False, True, False, False, False, False] # true if we want to keep the code of the variable in the final data
    new_col_name = ["custom_description", "country_of_origin", "country_of_arrival", "port_of_boarding", "mean_of_transport", "agente", "bank", "warehouse", "state"]
    key_as_int = [True, False, False, False, True, True, True, False, False ]
    

    # [source_col_names, dest_col_name, string, operation]
    combining_peru = [ [["FOB_DOLPOL",  "FLE_DOLAR"], "price_transport_net", False, "+"],      
                        [["price_transport_net",  "SEG_DOLAR"], "price_transport_net_insurance", False, "+"], 
                        [["FOB_DOLPOL", "UNID_FIQTY"], "net_price_per_unit", False, "/"],   
                        [["DESC_COMER", "DESC_MATCO", "DESC_USOAP", "DESC_FOPRE", "DESC_OTROS"], "commercial_description", True, "+"]]
    #[old_names, new_names]
    changing_peru = [["CODI_ADUAN","custom"],
                    ["FECH_RECEP", "date"],
                    ["FOB_DOLPOL", "net_price"]]
        
        
        
    rdds = []
    rdds.append(rdd)
    n_merging = len(key_pos_in_meta)
    for i in range(0,n_merging):
        rdds.append(merging_data_meta(rdds[i], meta_file_name[i], key_pos_in_meta[i], name_pos_in_meta[i], key_name_in_data[i], keep_code[i], new_col_name[i], key_as_int[i], path_to_metadata, "peru")) 
        #rdds.append(rdd)
        
    # COMBINING COLUMNS
    source_col_names = [["FOB_DOLPOL",  "FLE_DOLAR"],["price_transport_net",  "SEG_DOLAR"], ["FOB_DOLPOL", "UNID_FIQTY"], ["DESC_COMER", "DESC_MATCO", "DESC_USOAP", "DESC_FOPRE", "DESC_OTROS"]]
    dest_col_name = [    "price_transport_net",     "price_transport_net_insurance",     "net_price_per_unit",          "commercial_description"]
    string = [False, False, False, True]
    operation = ["+", "+", "/", "+"]
    
    
    for i in range(0, len(dest_col_name)):
        rdds.append(create_composed_columns(rdds[n_merging + i], source_col_names[i], dest_col_name[i], string[i], operation[i])) 
    
    # CHANGING NAMES
    n_combining = len(source_col_names)
    
    old_names = ["CODI_ADUAN", "FECH_RECEP", "FOB_DOLPOL"]
    new_names = ["custom", "date", "net_price"]
    rdds.append(rdds[n_combining + n_merging].map(lambda l: changeNames(l, old_names, new_names)))
    
    
    # SELECT THE COLUMNS TO BE KEPT
    to_be_kept = ['country_of_arrival', 'mean_of_transport', 'price_transport_net', 'price_transport_net_insurance', 'net_price_per_unit', 'commercial_description', 'custom', 'date', 'net_price']
    rdds.append(rdds[n_combining + n_merging + 1].map(lambda l: keepOnly(l, to_be_kept)))

    # ADDING COUNTRY NAME and "IMP" label
    rdds.append(rdds[n_combining + n_merging + 2].map(lambda l: insert(l, "country", "peru")).map(lambda l: insert(l, "type", "IMP")))
    
    # print(rdds[n_combining + n_merging + 3].take(1), "\n\n\n")
    return(rdds[n_combining + n_merging + 3])
#------------------------------------------------------

#------------------------------------------------------
# GENERAL PIPELINE
#------------------------------------------------------
def general_main(country, data_type, merging, combining, changing, path_to_avro, path_to_metadata):
    
    if country not in ["peru", "brazil", "chile"]:
        print("ERROR in COUNTRY NAME. It must be 'peru', 'chile' or 'brazil'")
        return -1
    if data_type not in ["IMP", "EXP"]:
        print("ERROR in DATA_TYPE . It must be 'IMP' or 'EXP'")
        return -1
    
    df = spark.read.format('avro').load(path_to_avro)
    rdd=df.rdd
    
    # print(rdd.take(1))
    iteration_merging = 0
    rdds = []
    rdds.append(rdd)
    
    # MERGING DATA AND META
    if len(merging)>0:
        meta_file_name = [row[0] for row in merging]
        key_pos_in_meta = [row[1] for row in merging]
        name_pos_in_meta = [row[2] for row in merging]
        key_name_in_data = [row[3] for row in merging]
        keep_code = [row[4] for row in merging]
        new_col_name = [row[5] for row in merging]
        key_as_int = [row[6] for row in merging]
        #print("meta_file_name", meta_file_name)
        #print("key_pos_in_meta", key_pos_in_meta)
        #print("name_pos_in_meta", name_pos_in_meta)
        #print("key_name_in_data", key_name_in_data)
        #print("keep_code", keep_code)
        #print("new_col_name", new_col_name)
        #print("key_as_int", key_as_int)

        for i in range(0,len(key_pos_in_meta)):
            rdds.append(merging_data_meta(rdds[i], meta_file_name[i], key_pos_in_meta[i], name_pos_in_meta[i], key_name_in_data[i], keep_code[i], new_col_name[i], key_as_int[i], path_to_metadata, "brasil")) 
            iteration_merging = iteration_merging + 1
            #print(rdds[i].take(1))


    # COMBINING COLUMNS
    iteration_combining = 0
    if len(combining)>0:
        source_col_names = [row[0] for row in combining]
        dest_col_name = [row[1] for row in combining]
        string = [row[2] for row in combining]
        operation = [row[3] for row in combining]

        for i in range(0, len(dest_col_name)):
            rdds.append(create_composed_columns(rdds[iteration_merging + i], source_col_names[i], dest_col_name[i], string[i], operation[i])) 
            iteration_combining = iteration_combining + 1

    # CHANGING NAMES
    n_combining = len(source_col_names)
    
    old_names = [row[0] for row in changing]
    new_names = [row[1] for row in changing]
    rdds.append(rdds[iteration_merging + iteration_combining].map(lambda l: changeNames(l, old_names, new_names)))
    
    # SELECT THE COLUMNS TO BE KEPT
    to_be_kept = ['country_of_arrival', 'mean_of_transport', 'price_transport_net', 'price_transport_net_insurance', 'net_price_per_unit', 'commercial_description', 'custom', 'date', 'net_price']
    rdds.append(rdds[iteration_merging + iteration_combining + 1].map(lambda l: keepOnly(l, to_be_kept)))
    
    # ADDING COUNTRY NAME and "IMP" label
    rdds.append(rdds[iteration_merging + iteration_combining + 2].map(lambda l: insert(l, "country", country)).map(lambda l: insert(l, "type", data_type)))
    # print(rdds[iteration_merging + iteration_combining + 3].take(1))
    return(rdds[iteration_merging + iteration_combining + 3])
#------------------------------------------------------

#------------------------------------------------------
# SEMANTICS OF THE DATA
#------------------------------------------------------
def user_define_formatting(country):
    
    if country =="brazil":
        # BRASIL

        # [meta_file_name, key_pos_in_meta, name_pos_in_meta, key_name_in_data, keep_code, new_col_name, key_as_int]
        merging_brazil=[["NCM.csv", 0, 1, "CO_NCM", False, "custom_description", True],
                          ["URF.csv", 0, 1, "CO_URF", False, "custom", True],
                          ["PAIS.csv", 0, 4, "CO_PAIS",False, "country_of_arrival", True],
                          ["VIA.csv", 0, 1, "CO_VIA", False, "mean_of_transport", True],
                          ["UF.csv", 1, 2, "SG_UF_NCM", False, "arrival_place", False]]
        # [source_col_names, dest_col_name, string, operation]
        combining_brazil = [ [["VL_FOB", "VL_FRETE"], "price_transport_net", False, "+"],
                           [["price_transport_net",  "VL_SEGURO"], "price_transport_net_insurance", False, "+"],
                           [["VL_FOB", "KG_LIQUIDO"], "net_price_per_unit", False, "/"],
                           [["CO_MES", "CO_ANO"], "date", True, "+"]]
        #[old_names, new_names]
        changing_brazil = [["VL_FOB","net_price"],
                           ["custom_description","commercial_description" ]]
        path_to_avro_brazil = "/Users/pietro/Desktop/BDM/Project/DataImporta/P2/development/test_data/test_brazil.avro"
        path_to_metadata_brazil = '/Users/pietro/Desktop/BDM/Project/Data.nosync/brazil/metadata copia/'

        return general_main("brazil", "IMP", merging_brazil, combining_brazil, changing_brazil, path_to_avro_brazil, path_to_metadata_brazil)


        
    if country =="chile":
        # CHILE

        merging_chile=[]
        combining_chile = [ [["FOB", "FLETE"],"price_transport_net", False, "+"],
                            [["price_transport_net",  "SEGURO"], "price_transport_net_insurance", False, "+"],
                            [['DNOMBRE', 'DMARCA', 'DVARIEDAD', 'DOTRO1', 'DOTRO2', 'ATR_5',  'ATR_6'],"commercial_description", True, "+"] ]
        #[old_names, new_names]
        changing_chile = [["ADU", "custom"],
                           ["FECVENCI", "date"],
                           ["FOB", "net_price"],
                           ["PRE_UNIT", "net_price_per_unit"],
                           ["CODPAISCON", "country_of_arrival"],
                           ["VIA_TRAN", "mean_of_transport"]]

        path_to_avro_chile = "/Users/pietro/Desktop/BDM/Project/DataImporta/P2/development/test_data/test_chile.avro"
        path_to_metadata_chile= ''

        return general_main("chile", "IMP", merging_chile, combining_chile, changing_chile, path_to_avro_chile, path_to_metadata_chile)
    
    if country=="peru": # NOT WORKING!!!!!!

        # [meta_file_name, key_pos_in_meta, name_pos_in_meta, key_name_in_data, keep_code, new_col_name, key_as_int]
        merging_peru=[["Partidas.txt", 0, 1, "PART_NANDI", True, "custom_description", True],
                      ["Paises.txt", 0, 1,  "PAIS_ORIGE", False, "country_of_origin", False],
                      [ "Paises.txt", 0, 1, "PAIS_ADQUI", False, "country_of_arrival", False],
                      ["Puertos.txt", [0,2], 3, "PUER_EMBAR", False, "port_of_boarding", False],
                      ["MedTransporte.txt", 0, 1, "VIA_TRANSP", True, "mean_of_transport", True],
                      ["Agente.txt", 0, 1, "CODI_AGENT", False, "agente", True],
                      [ "Bancos.txt", 0, 1, "BANC_CANCE", False, "bank", True],
                      ["RecintAduaner.txt", 0, 1, "CODI_ALMA", False, "warehouse", False],
                      ["EstMercancia.txt", 0, 1, "SEST_MERCA", False, "state", False]
                     ]
        # [source_col_names, dest_col_name, string, operation]
        combining_peru = [ [["FOB_DOLPOL",  "FLE_DOLAR"], "price_transport_net", False, "+"],      
                          [["price_transport_net",  "SEG_DOLAR"], "price_transport_net_insurance", False, "+"], 
                          [["FOB_DOLPOL", "UNID_FIQTY"], "net_price_per_unit", False, "/"],   
                          [["DESC_COMER", "DESC_MATCO", "DESC_USOAP", "DESC_FOPRE", "DESC_OTROS"], "commercial_description", True, "+"]]
        #[old_names, new_names]
        changing_peru = [["CODI_ADUAN","custom"],
                        ["FECH_RECEP", "date"],
                        ["FOB_DOLPOL", "net_price"]]
        path_to_avro_peru =  "/Users/pietro/Desktop/BDM/Project/DataImporta/P2/development/test_data/version0.avro"
        path_to_metadata_peru = '/Users/pietro/Desktop/BDM/Project/Data.nosync/peru/metadata_copia/'
        
        return general_main("peru", "IMP", merging_peru, combining_peru, changing_peru, path_to_avro_peru, path_to_metadata_peru)
#------------------------------------------------------


#------------------------------------------------------
# LOAD DATA TO POSTGRESQL
#------------------------------------------------------
def main():
    data_chile=user_define_formatting("chile")
    chileDF = data_chile.toDF()

    data_brazil=user_define_formatting("brazil")
    brazilDF = data_brazil.toDF()

    data_peru=main_peru()
    # data_peru = user_define_formatting("peru")
    peruDF = data_peru.toDF()

    properties = {"user": "pietro", "password": "ropby8pietro", "driver": 'org.postgresql.Driver'}
    url = "jdbc:postgresql://localhost:5432/dataimporta"

    chileDF.write.format("jdbc").mode("append").jdbc(url,"all_countries",
              properties = properties)
    brazilDF.write.format("jdbc").mode("append").jdbc(url,"all_countries",
              properties = properties)
    peruDF.write.format("jdbc").mode("append").jdbc(url,"all_countries",
              properties = properties)
#------------------------------------------------------


# RUN THE SCRIPT
main()