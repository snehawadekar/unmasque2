import os
import sys
import csv
import copy
import math
import executable
import pandas as pd

sys.path.append('../') 
import reveal_globals
import psycopg2

import where_clause

def getCoreSizes(core_relations):
    print("DB_MINIMIZER.getcoresizes")
    core_sizes = {}
    for tabname in core_relations:
        try:
            cur = reveal_globals.global_conn.cursor()
            cur.execute('select count(*) from ' + tabname + ';')
            res = cur.fetchone()#it will return a tuple
            cur.close()
            core_sizes[tabname] = int(str(res[0]))
            print("core sizes==",core_sizes)
        except Exception as error:
            print(type(error))
            reveal_globals.error="Unmasque:\n Error in getting table Sizes. \n Postgres Error: \n" + dict(error.args[0])['M']
            print("Error in getting table Sizes. Error: " + str( error))
    return core_sizes

def getTableAttri(core_relations):
	get_attri_dict = {}
	for i in core_relations:
		cur = reveal_globals.global_conn.cursor()
		cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' and TABLE_CATALOG='december' and table_name="+"'"+i+"'"+";")
		res = cur.fetchall()
		cur.close()
		temp_list=[]
		for j in res:
			temp_list.append(str(j[0]))
		get_attri_dict[i] = temp_list	
	return get_attri_dict




def sample_Database_Instance(core_relations, sample_size_percent, sample_threshold, max_sample_iter, executable_path = ""):
	#get core sizes
	print("dbminimizer.sample_database_instance")
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
				cur = reveal_globals.global_conn.cursor()
				cur.execute("Alter table " + tabname + " rename to " + tabname + "1;")
				cur.execute("create unlogged table " + tabname + " (like " + tabname + "1);")
				cur.execute("Insert into " + tabname + " select * from " + tabname + "1 where random() < .1 limit " + str(temp_sample_size) + ";")
				cur.execute('alter table ' + tabname + ' add primary key(' + reveal_globals.global_pk_dict[tabname] + ');') #it will add primary key to the table
				print("Sampling table " + tabname, flush = True)
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
				'''m=getTableAttri(core_relations)
				print(m)'''
				#Run query and analyze the result now
				new_result = executable.getExecOutput()
				reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
				if len(new_result) <= 1:
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

#copy based Minimizer
def reduce_Database_Instance(core_relations, method = 'binary partition', max_no_of_rows = 1, executable_path = ""):
    print("dbminimizer.reduce_Database_Instance")
    reveal_globals.local_other_info_dict = {}
    #Perform sampling and return the table size
    core_sizes = sample_Database_Instance(core_relations, reveal_globals.global_sample_size_percent, reveal_globals.global_sample_threshold, reveal_globals.global_max_sample_iter)
    #core_sizes = getCoreSizes(core_relations)
    #print(core_sizes)
    #STORE STARTING POINT(OFFSET) AND NOOFROWS(LIMIT) FOR EACH TABLE IN FORMAT (offset, limit)
    partition_dict = {}
    no_project_att_tab = []
    for key in core_sizes.keys():
        partition_dict[key] = (0, core_sizes[key]) #initially it will store 0 for offset and no. of rows for each table 
    #print(partition_dict)
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
        new_result = executable.getExecOutput()
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
        if len(new_result) > 1:
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
    # if not os.path.exists(reveal_globals.global_reduced_data_path):
    # 	os.makedirs(reveal_globals.global_reduced_data_path)
    print("reached here")
    # for tabname in core_relations:
    # 	cur = reveal_globals.global_conn.cursor()
    # 	cur.execute("copy " + tabname + " to " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
    # 	cur.close()

    for tabname in core_relations:
        cur = reveal_globals.global_conn.cursor()
        # cur.execute('drop table'+ tabname + "4 )
        cur.execute("create table " + tabname + "4 as select * from " + tabname + ";")
        cur.close()

    # for tabname in core_relations:
    # 	# cur = reveal_globals.global_conn.cursor()
    # 	#CHANGES MADE 29 JUNE
    # 	conn=reveal_globals.global_conn
    # 	sql_query = pd.read_sql_query("select * from "+tabname+";",conn)
    # 	df = pd.DataFrame(sql_query)
    # 	df.to_csv (tabname+'_rd.csv', index = False) 
    # 	print("WRITTEN INTO CSV FILE")
        # with open(tabname+'_rd.csv', 'w',newline='') as f:
        # 	print("reached here")
        # 	cur=reveal_globals.global_conn.cursor()
        # 	print("reached here2")
        # 	r=cur.execute("select * from "+tabname+";")
        # 	print("reached here2")
        # 	data=r.fetchall()
        # 	print("reached here2")
        # 	print(data)
        # 	csv_writer = csv.writer(f)
        # 	csv_writer.writerows(data)
        # 	print("WRITTEN INTO CSV FILE")



        # # fid.close()
        # cur.execute("\copy " + tabname + " to '"+tabname + "_rd.csv' " + "DELIMITER ',' CSV HEADER;")
        # cur.close()
    #SANITY CHECK
    new_result = executable.getExecOutput()
    if len(new_result) <= 1:
        reveal_globals.error="Unmasque Error: \n Hidden Query does not produce populated result on User Database\n"
        print("Error: Query out of extractable domain\n")
        return False
        
    #populate screen data
    #POPULATE MIN INSTANCE DICT
    # for tabname in reveal_globals.global_core_relations:
    # 	reveal_globals.global_min_instance_dict[tabname] = []
    # 	with open( tabname + '_rd.csv', 'rt') as f: #1 july changes
    # 		data = csv.reader(f)
    # 		for row in data:
    # 			reveal_globals.global_min_instance_dict[tabname].append(tuple(row))
    conn=reveal_globals.global_conn
    for tabname in reveal_globals.global_core_relations:
        reveal_globals.global_min_instance_dict[tabname] = []
        sql_query = pd.read_sql_query("select * from "+tabname+";",conn)
        df = pd.DataFrame(sql_query)
        reveal_globals.global_min_instance_dict[tabname].append(tuple(df.columns))
        for index, row in df.iterrows():
            reveal_globals.global_min_instance_dict[tabname].append(tuple(row))
    # print(reveal_globals.global_min_instance_dict)
    # dict = {}
    # for tabname in reveal_globals.global_core_relations:
    # 	dict[tabname] = []
    # 	cur = reveal_globals.global_conn.cursor()
    # 	cur.execute("Select * From " + tabname + "4 where false;")
    # 	var = cur.fetchall()
    # 	cur.close()
    # 	dict[tabname].append(tuple(var))
    # 	cur = reveal_globals.global_conn.cursor()
    # 	cur.execute("Select * From " + tabname + "4;")
    # 	data = cur.fetchall()
    # 	cur.close()
    # 	for row in data:
    # 		dict[tabname].append(tuple(row))

    # print(dict)		
    # print("******")
    #reveal_globals.global_min_instance_dict[tabname].append(tuple(row))			
    #populate other data
    reveal_globals.global_result_dict['min'] = copy.deepcopy(new_result)
    reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)
    reveal_globals.global_other_info_dict['min'] = copy.deepcopy(reveal_globals.local_other_info_dict)
    return True



# view-based minimizer
def reduce_Database_Instance3(core_relations, method = 'binary partition', max_no_of_rows = 1, executable_path = ""):
    print("db_minimizer.reduce_Database_Instance3")
    reveal_globals.local_other_info_dict = {}
    #Perform sampling
    #print(core_relations, reveal_globals.global_sample_size_percent, reveal_globals.global_sample_threshold, reveal_globals.global_max_sample_iter,"++++SAMPLING++++++++")
    core_sizes = sample_Database_Instance(core_relations, reveal_globals.global_sample_size_percent, reveal_globals.global_sample_threshold, reveal_globals.global_max_sample_iter)
    core_sizes = getCoreSizes(core_relations)
    #STORE STARTING POINT(OFFSET) AND NOOFROWS(LIMIT) FOR EACH TABLE IN FORMAT (offset, limit)
    partition_dict = {}
    for key in core_sizes.keys():
        partition_dict[key] = (0, core_sizes[key])

    for tabname in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('drop table ' + tabname + ';')
        cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1;')
        cur.close()

    #indexnumber as index has to be unique each time
    index_no = 50
    while(1):
        if(not bool(core_sizes)):
            break
        key_max = max(core_sizes.keys(), key=(lambda k: core_sizes[k]))
        tabname = key_max
            
        #Move half of the data from temp to x
        cur = reveal_globals.global_conn.cursor()
        cur.execute('drop view '+ tabname + ';')
        #cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1 order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[tabname][0])) + ' limit ' + str(int(partition_dict[tabname][1]/2)) + ';')
        cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1 offset ' + str(int(partition_dict[tabname][0])) + ' limit ' + str(int(partition_dict[tabname][1]/2)) + ';')
        cur.close()

        #Set statement timeout to 2hours
        cur = reveal_globals.global_conn.cursor()
        cur.execute('set statement_timeout to "120min"')
        cur.close()

        #Run query and analyze the result now
        new_result = executable.getExecOutput()
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
        if len(new_result) > 1:
            partition_dict[tabname] = (0, int(partition_dict[tabname][1]/2))
        else:
            partition_dict[tabname] = ( partition_dict[tabname][0] + int(partition_dict[tabname][1]/2), int(partition_dict[tabname][1]) - int(partition_dict[tabname][1]/2) )
        core_sizes[tabname] = int(partition_dict[tabname][1])
        key_max = max(core_sizes.keys(), key=(lambda k: core_sizes[k]))
        new_result = executable.getExecOutput()
        print("###LENGTH###",len(new_result))
        #WHEN IT IS LESS THAN OR EQUAL TO MAX PERMISSIBLE ROWS
        if core_sizes[tabname] <= max_no_of_rows:
            print("spot-1")
            #Convert the view into table
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Alter view ' + tabname + ' rename to temp;')
            cur.close()

            cur = reveal_globals.global_conn.cursor()
            cur.execute('create table ' + tabname + ' as Select * from temp;')
            cur.close()

            cur = reveal_globals.global_conn.cursor()
            cur.execute('Drop view temp; ')
            cur.close()
            del core_sizes[tabname]
    #WRITE TO Reduced Data Directory
    #check for data directory existence, if not exists , create it
    if not os.path.exists(reveal_globals.global_reduced_data_path):
        os.makedirs(reveal_globals.global_reduced_data_path)
    for tabname in core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("copy " + tabname + " to " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.close()

    
    # for tabname in core_relations:
    #     cur = reveal_globals.global_conn.cursor()
    #     # cur.execute('drop table'+ tabname + "4 )
    #     cur.execute("create table " + tabname + "4 as select * from " + tabname + ";")
    #     cur.close()

    #SANITY CHECK
    new_result = executable.getExecOutput()
    if len(new_result) <= 1:
        print("Error: Query out of extractable domain\n")
        # return False
    #populate screen data
    #POPULATE MIN INSTANCE DICT
    for tabname in reveal_globals.global_core_relations:
        reveal_globals.global_min_instance_dict[tabname] = []
        with open(reveal_globals.global_reduced_data_path + tabname + '.csv', 'rt') as f:
            data = csv.reader(f)
            for row in data:
                reveal_globals.global_min_instance_dict[tabname].append(tuple(row))

    conn=reveal_globals.global_conn
    for tabname in reveal_globals.global_core_relations:
        reveal_globals.global_min_instance_dict[tabname] = []
        sql_query = pd.read_sql_query("select * from "+tabname+";",conn)
        df = pd.DataFrame(sql_query)
        reveal_globals.global_min_instance_dict[tabname].append(tuple(df.columns))
        for index, row in df.iterrows():
            reveal_globals.global_min_instance_dict[tabname].append(tuple(row))
    #populate other data
    reveal_globals.global_result_dict['min'] = copy.deepcopy(new_result)
    reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)
    reveal_globals.global_other_info_dict['min'] = copy.deepcopy(reveal_globals.local_other_info_dict)
    return True


