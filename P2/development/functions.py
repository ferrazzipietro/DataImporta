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
