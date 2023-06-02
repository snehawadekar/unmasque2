import os
import sys
import time
import copy
import math
import executable
import check_nullfree 
import reveal_globals
import pandas as pd

def getCoreSizes(core_relations):
	# core_sizes = {}
	# for tabname in core_relations:
	# 	try:
	# 		cur = reveal_globals.global_conn.cursor()
	# 		cur.execute('select count(*) from ' + tabname + ';')
	# 		res = cur.fetchone()
	# 		cur.close()
	# 		core_sizes[tabname] = int(str(res[0]))
	# 	except Exception as error:
	# 		print("Error in getting table Sizes. Error: " + str(error))
   
    sf = 1
    reveal_globals.global_core_sizes= {
        'nation' : 25,
        'region' :5,
        'part' : 200000* sf,
        'partsupp' : 800000 * sf,
        'lineitem' : 6000000 * sf,
        'orders' : 1500000 * sf,
        'supplier' : 10000 * sf,
        'customer' : 150000 * sf,
        
    }
    core_sizes = {}
    for tabname in core_relations:
        core_sizes[tabname] = reveal_globals.global_core_sizes[tabname]
    return core_sizes

def sample_Database_Instance(core_relations, sample_size_percent, sample_threshold, max_sample_iter, executable_path = ""):
    #get core sizes
    core_sizes = getCoreSizes(core_relations)
    
    #update other info dict
    for tabname in core_relations:
        reveal_globals.local_other_info_dict[u'Initial / Final Row Cardinality \u2014 Table ' + tabname] = str(core_sizes[tabname]) + " / 1"
    #SAMPLE TABLE WITH > sample_threshold ROWS TO sample_size ROWS (max size in each iteration)
    temp_core_sizes = copy.deepcopy(core_sizes)
    index_no = 20
    while bool(temp_core_sizes):
        temp_sample_size_percent = sample_size_percent
        key_max = max(temp_core_sizes.keys(), key=(lambda k: temp_core_sizes[k]))
        if temp_core_sizes[key_max] > sample_threshold:
            for sample_iter in range(max_sample_iter):
                tabname = key_max
                temp_sample_size = math.floor((core_sizes[tabname] * temp_sample_size_percent)/100)
                print("Sampling table " + tabname, flush = True)
                cur = reveal_globals.global_conn.cursor()
                cur.execute("drop table if exists "+ tabname + "1;" )
                cur.execute("Alter table " + tabname + " rename to " + tabname + "1;")
                cur.execute("create unlogged table " + tabname + " (like " + tabname + "1);")
                cur.execute("Insert into " + tabname + " select * from " + tabname + "1 where random() < .1 limit " + str(temp_sample_size) + ";")
                cur.execute('alter table ' + tabname + ' add primary key(' + reveal_globals.global_pk_dict[tabname] + ');')
                for elt in reveal_globals.global_index_dict[tabname]:
                    index_flag = False
                    while index_flag == False:
                        try:
                            cur.execute('create index ' + tabname + str(index_no) + ' ON ' + elt + ';')
                            index_flag = True
                        except:
                            index_no = index_no + 1
                    index_no = index_no + 1
                cur.close()
                #Run query and analyze the result now
                # new_result = executable.getExecOutput()
                
                reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
                if check_nullfree.getExecOutput() == True : #if len(new_result) <= 1:
                    if (sample_iter == max_sample_iter - 1):
                        print("Sampling failed for " + tabname + " after "  + str(max_sample_iter) + ' iterations. Going for a full copy now.')
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('drop table ' + tabname + ';')
                        cur.execute("create unlogged table " + tabname + " (like " + tabname + "1);")
                        cur.execute("Insert into " + tabname + " select * from " + tabname + "1;")
                        cur.execute('alter table ' + tabname + ' add primary key(' + reveal_globals.global_pk_dict[tabname] + ');')
                        for elt in reveal_globals.global_index_dict[tabname]:
                            index_flag = False
                            while index_flag == False:
                                try:
                                    cur.execute('create index ' + tabname + str(index_no) + ' ON ' + elt + ';')
                                    index_flag = True
                                except:
                                    index_no = index_no + 1
                            index_no = index_no + 1
                        cur.close()
                    else:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute('drop table ' + tabname + ';')
                        cur.execute('alter table ' + tabname + '1 rename to ' + tabname + ';')
                        cur.close()
                        temp_sample_size_percent = temp_sample_size_percent + .2
                else:
                    print("Sampled " + tabname + " successfully")
                    core_sizes[tabname] = temp_sample_size
                    break
            del temp_core_sizes[key_max]
        else: #Make a copy of the table
            tabname = key_max
            cur = reveal_globals.global_conn.cursor()
            cur.execute("drop table if exists "+ tabname + "1;" )
            cur.execute("Alter table " + tabname + " rename to " + tabname + "1;")
            cur.execute("create unlogged table " + tabname + " (like " + tabname + "1);")
            cur.execute("Insert into " + tabname + " select * from " + tabname + "1;")
            cur.execute('alter table ' + tabname + ' add primary key(' + reveal_globals.global_pk_dict[tabname] + ');')
            for elt in reveal_globals.global_index_dict[tabname]:
                index_flag = False
                while index_flag == False:
                    try:
                        cur.execute('create index ' + tabname + str(index_no) + ' ON ' + elt + ';')
                        index_flag = True
                    except:
                        index_no = index_no + 1
                    index_no = index_no + 1
            cur.close()
            del temp_core_sizes[key_max]
    return core_sizes


def reduce_Database_Instance(core_relations, method = 'binary partition', max_no_of_rows = 1, executable_path = ""):
    reveal_globals.local_other_info_dict = {}
    #Perform sampling
    core_sizes = getCoreSizes(core_relations)
    # core_sizes = sample_Database_Instance(core_relations, reveal_globals.global_sample_size_percent, reveal_globals.global_sample_threshold, reveal_globals.global_max_sample_iter)
    print(check_nullfree.getExecOutput())
    #STORE STARTING POINT(OFFSET) AND NOOFROWS(LIMIT) FOR EACH TABLE IN FORMAT (offset, limit)
    partition_dict = {}
    for key in core_sizes.keys():
        partition_dict[key] = (0, core_sizes[key])
    #indexnumber as index has to be unique each time
    index_no = 50
    while(1):
        if(not bool(core_sizes)):
            break
        key_max = max(core_sizes.keys(), key=(lambda k: core_sizes[k]))
        tabname = key_max
        #Rename Current Table x to temp
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Alter table ' + tabname + ' rename to temp;')
        cur.close()
        #Create a empty table with name x
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Create unlogged table ' + tabname + ' (like temp);')
        cur.close()
        #Move half of the data from temp to x
        cur = reveal_globals.global_conn.cursor()
        cur.execute('Insert into ' + tabname + ' select * from temp order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[tabname][0])) + ' limit ' + str(int(partition_dict[tabname][1]/2)) + ';')
        cur.execute('alter table ' + tabname + ' add primary key(' + reveal_globals.global_pk_dict[tabname] + ');')
        for elt in reveal_globals.global_index_dict[tabname]:
            index_flag = False
            while index_flag == False:
                try:
                    cur.execute('create index ' + tabname + str(index_no) + ' ON ' + elt + ';')
                    index_flag = True
                except:
                    index_no = index_no + 1
            index_no = index_no + 1
        cur.close()
        #Run query and analyze the result now
        # new_result = executable.getExecOutput()
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
        # if len(new_result) > 1:
        if check_nullfree.getExecOutput() == True :
            cur = reveal_globals.global_conn.cursor()
            cur.execute('drop table temp;')
            partition_dict[tabname] = (0, int(partition_dict[tabname][1]/2))
            cur.close()
        else:
            cur = reveal_globals.global_conn.cursor()
            cur.execute('drop table ' + tabname + ';')
            cur.close()
            cur = reveal_globals.global_conn.cursor()
            cur.execute("Alter table temp rename to " + tabname + ";")
            cur.close()
            partition_dict[tabname] = ( partition_dict[tabname][0] + int(partition_dict[tabname][1]/2), int(partition_dict[tabname][1]) - int(partition_dict[tabname][1]/2) )
        core_sizes[tabname] = int(partition_dict[tabname][1])
        key_max = max(core_sizes.keys(), key=(lambda k: core_sizes[k]))
        #WHEN IT IS LESS THAN OR EQUAL TO MAX PERMISSIBLE ROWS
        if core_sizes[tabname] <= max_no_of_rows:
            #Rename Current Table x to temp
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Alter table ' + tabname + ' rename to temp;')
            cur.close()
            #Create a empty table with name x
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Create unlogged table ' + tabname + ' (like temp);')
            cur.close()
            #Move half of the data from temp to x
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Insert into ' + tabname + ' select * from temp order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[tabname][0])) + ' limit ' + str(int(partition_dict[tabname][1])) + ';')
            '''
            cur.execute('alter table ' + tabname + ' add primary key(' + reveal_globals.global_pk_dict[tabname] + ');')
            for elt in reveal_globals.global_index_dict[tabname]:
                index_flag = False
                while index_flag == False:
                    try:
                        cur.execute('create index ' + tabname + str(index_no) + ' ON ' + elt + ';')
                        index_flag = True
                    except:
                        index_no = index_no + 1
                index_no = index_no + 1
            '''
            cur.close()
            cur = reveal_globals.global_conn.cursor()
            cur.execute('drop table temp;')
            cur.close()
            del core_sizes[tabname]
    #WRITE TO Reduced Data Directory
    #check for data directory existence, if not exists , create it
    if not os.path.exists(reveal_globals.global_reduced_data_path):
        os.makedirs(reveal_globals.global_reduced_data_path)
    for tabname in core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("drop table if exists "+ tabname + "4;" )
        cur.execute("create table " + tabname + "4 as select * from " + tabname + ";")
        # cur.execute("copy " + tabname + " to " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.close()
    #SANITY CHECK
    cur = reveal_globals.global_conn.cursor()
    query=reveal_globals.query1
    cur.execute(query)
    res = cur.fetchall() 
    print("Results afted DB minimization")
    print(res)
    cur.close()
    new_result = executable.getExecOutput()
    # if len(new_result) <= 1:
    if check_nullfree.getExecOutput() == False :
        print("Error: Query out of extractable domain\n")
        return False
    #populate screen data
    #POPULATE MIN INSTANCE DICT
    reveal_globals.copy_min_time=time.time()- reveal_globals.local_start_time
    conn=reveal_globals.global_conn
    for tabname in reveal_globals.global_core_relations:
        reveal_globals.global_min_instance_dict[tabname] = []
        sql_query = pd.read_sql_query("select * from "+tabname+";",conn)
        df = pd.DataFrame(sql_query)
        reveal_globals.global_min_instance_dict[tabname].append(tuple(df.columns))
        for index, row in df.iterrows():
            reveal_globals.global_min_instance_dict[tabname].append(tuple(row))
    #populate other data
    new_result = executable.getExecOutput()
    reveal_globals.global_result_dict['min'] = copy.deepcopy(new_result)
    reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)
    reveal_globals.global_other_info_dict['min'] = copy.deepcopy(reveal_globals.local_other_info_dict)
    return True