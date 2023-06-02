import os
import sys
try:
	import psycopg2
except ImportError:
	pass

sys.path.append('../')

import reveal_globals
import datetime
import csv
import copy
import math
import executable

def solve_count():
    print("#####Count Handler#####")
    bool_val = reduce_Database_Instance_aman(reveal_globals.global_core_relations)
    if bool_val:
        print("done minimization")
        filter = get_filter_predicates()
        print("Filter: ", filter)
    else:
        print("Query out of Extractable Domain by Aman")
    return

def getCoreSizesAman(core_relations):
    core_sizes = {}
    for tabname in core_relations:
        try:
            cur = reveal_globals.global_conn.cursor()
            cur.execute('select count(*) from ' + tabname + ';')
            res = cur.fetchone()
            cur.close()
            core_sizes[tabname] = int(str(res[0]))
        except Exception as error:
            print("Error in getting table Sizes. Error: " + str(error))
    return core_sizes

def reduce_Database_Instance_aman(core_relations, method = 'binary partition', max_no_of_rows = 1, executable_path = ""):
	for tabname in reveal_globals.global_core_relations:
		cur = reveal_globals.global_conn.cursor()
		cur.execute('Delete from ' + tabname + ';')
		cur.execute('Insert into ' + tabname + ' select * from ' + tabname + '4;')
		cur.close()
	core_sizes = getCoreSizesAman(reveal_globals.global_core_relations)
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
		new_result = executable.getExecOutput()
		reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
		# if new_result[1][0] != '0': #len(new_result) > 1:
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
			cur.close()
			cur = reveal_globals.global_conn.cursor()
			cur.execute('drop table temp;')
			cur.close()
			del core_sizes[tabname]
	#WRITE TO Reduced Data Directory
	#check for data directory existence, if not exists , create it

	for tabname in core_relations:
		cur = reveal_globals.global_conn.cursor()
		cur.execute("drop table if exists "+ tabname + "4;" )
		cur.execute("create table " + tabname + "4 as select * from " + tabname + ";")
		cur.close()
	#SANITY CHECK
	# new_result = executable.getExecOutput()
	# print("Result: ", new_result, new_result[1][0])
	# if len(new_result) <= 1:
	#     print("Error: Query out of extractable domain\n")
	#     return False
	#populate screen data
	#POPULATE MIN INSTANCE DICT
	# for tabname in reveal_globals.global_core_relations:
	# 	reveal_globals.global_min_instance_dict[tabname] = []
	# 	with open(reveal_globals.global_reduced_data_path + tabname + '.csv', 'rt') as f:
	# 		data = csv.reader(f)
	# 		for row in data:
	# 			reveal_globals.global_min_instance_dict[tabname].append(tuple(row))
	#populate other data
	reveal_globals.global_result_dict['min'] = copy.deepcopy(new_result)
	reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)
	reveal_globals.global_other_info_dict['min'] = copy.deepcopy(reveal_globals.local_other_info_dict)
	return True

#SUPPORT FUNCTIONS FOR FILTER PREDICATES

def update_other_data(tabname, attrib, attrib_type, val, result, other_info_list):
	reveal_globals.local_other_info_dict = {}
	if 'text' not in attrib_type and other_info_list != []:
		low = str(other_info_list[0])
		mid = str(other_info_list[1])
		high = str(other_info_list[2])
		low_next = str(other_info_list[3])
		high_next = str(other_info_list[4])
		reveal_globals.local_other_info_dict['Current Search Range'] = '[' + low + ', '+high+']'
		reveal_globals.local_other_info_dict['Current Mutation'] = 'Mutation of attribute ' + attrib + ' with value '+ str(val)
		reveal_globals.local_other_info_dict['Result Cardinality'] = (len(result) - 1)
		reveal_globals.local_other_info_dict['New Search Range'] = '[' + low_next + ', '+high_next+']'
	else:
		reveal_globals.local_other_info_dict['Current Mutation'] = 'Mutation of attribute ' + attrib + ' with value '+ str(val)
		reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(result) - 1)
	temp = copy.deepcopy(reveal_globals.global_min_instance_dict[tabname])
	index = temp[0].index(attrib)
	mutated_list = copy.deepcopy(list(temp[1]))
	mutated_list[index] = str(val)
	temp[1] = mutated_list
	for tab in reveal_globals.global_core_relations:
		reveal_globals.global_min_instance_dict['filter_'+attrib+'_'+tab+'_D_mut'+str(reveal_globals.local_instance_no)] = reveal_globals.global_min_instance_dict[tab]
	reveal_globals.global_min_instance_dict['filter_'+attrib+'_'+tabname+'_D_mut'+str(reveal_globals.local_instance_no)] = temp
	reveal_globals.global_result_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no)] = copy.deepcopy(result)
	reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(result) - 1)
	reveal_globals.local_instance_list.append('D_mut'+str(reveal_globals.local_instance_no))
	reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no)] = copy.deepcopy(reveal_globals.local_other_info_dict)
	reveal_globals.local_instance_no += 1
	

def is_int(s):
	try:
		int(s)
		return True
	except ValueError:
		return False

def get_init_data():
	#Get all attributes, attribute types, max length and reduced db content
	reveal_globals.global_attrib_types = []
	reveal_globals.global_all_attribs = []
	reveal_globals.global_d_plus_value = {}
	reveal_globals.global_attrib_max_length = {}
	for tabname in reveal_globals.global_core_relations:
		'''
		#query to get key attributes
		query = "SELECT tc.table_schema,tc.constraint_name,tc.table_name,kcu.column_name,ccu.table_schema AS foreign_table_schema,\
		ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name FROM \
		information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name\
		AND tc.table_schema = kcu.table_schema JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name\
		AND ccu.table_schema = tc.table_schema\
		WHERE (tc.constraint_type = 'PRIMARY KEY' OR tc.constraint_type = 'FOREIGN KEY') AND tc.table_name = '" + tabname + "';"
		cur = reveal_globals.global_conn.cursor()
		cur.execute(query)
		res = cur.fetchall()
		cur.close()
		'''
		query = "select column_name, data_type, character_maximum_length from information_schema.columns where table_name = '" + tabname + "'; "
		cur = reveal_globals.global_conn.cursor()
		cur.execute(query)
		res = cur.fetchall()
		cur.close()
		tab_attribs = []
		for row in res:
			tab_attribs.append(row[0])
			reveal_globals.global_attrib_types.append((tabname,row[0],row[1]))
			if(is_int(str(row[2]))):
				reveal_globals.global_attrib_max_length[(tabname, row[0])] = int(str(row[2]))
		reveal_globals.global_all_attribs.append(copy.deepcopy(tab_attribs))
		#WRITE CODE TO GET INTEGER/DATE/FLOAT MIN AND MAX
		cur = reveal_globals.global_conn.cursor()
		query = 'select ' + tab_attribs[0]
		for i in range(1,len(tab_attribs)):
			query = query + ", " + tab_attribs[i]
		query = query + ' from ' + tabname + ';'
		cur.execute(query)
		res = cur.fetchall()
		cur.close()
		for row in res:
			for i in range(len(tab_attribs)):
				reveal_globals.global_d_plus_value[tab_attribs[i]] = row[i]

def checkAttribValueEffect(tabname, attrib, val):
	#updatequery
	query = "update " + tabname + " set " + attrib + " = " + str(val) + ";"
	cur = reveal_globals.global_conn.cursor()
	cur.execute(query)
	cur.close()
	new_result = executable.getExecOutput()
	# if new_result[1][0] == '0': 
	if len(new_result) <= 1:
		cur = reveal_globals.global_conn.cursor()
		cur.execute("Truncate Table " + tabname + ";")
		cur.close()
		cur = reveal_globals.global_conn.cursor()
		cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
		# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
		cur.close()
	update_other_data(tabname, attrib, 'int', val, new_result, [])
	# if new_result[1][0] != '0': 
	if len(new_result) > 1:
		return True
	return False

def getIntFilterValue(tabname, filter_attrib, min_val, max_val, operator):
	counter = 0
	query_front = "update " + tabname + " set " + filter_attrib + " = "
	query_back = ""
	firstflag = True
	cur = reveal_globals.global_conn.cursor()
	cur.execute("Truncate Table " + tabname + ";")
	#conn.commit()
	cur.close()
	cur = reveal_globals.global_conn.cursor()
	cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
	# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
	#conn.commit()
	cur.close()
	if operator == '<=':
		low = min_val
		high = max_val
		while (high - low) >0:
			mid_val = int(math.ceil((low + high)/2))
			#updatequery
			query =  query_front + " " + str(mid_val) + " " + query_back + ";"
			cur = reveal_globals.global_conn.cursor()
			cur.execute(query)
			cur.close()
			new_result = executable.getExecOutput()
			if new_result[1][0] == '0': #len(new_result) <= 1:
    			#put filter_
				update_other_data(tabname, filter_attrib, 'int', mid_val, new_result, [low, mid_val, high, low, mid_val-1])
				high = mid_val - 1
			else:
    			#put filter_
				update_other_data(tabname, filter_attrib, 'int', mid_val, new_result, [low, mid_val, high, mid_val, high])
				low = mid_val
			cur = reveal_globals.global_conn.cursor()
			cur.execute('TRUNCATE table ' + tabname + ';')
			cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
			# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
			cur.close()
		return str(low)
	if operator == '>=':
		low = min_val
		high = max_val
		while (high - low) >0:
			mid_val = int((low + high)/2)
			#updatequery
			query =  query_front + " " + str(mid_val) + " " + query_back + ";"
			cur = reveal_globals.global_conn.cursor()
			cur.execute(query)
			cur.close()
			new_result = executable.getExecOutput()
			if new_result[1][0] == '0': #len(new_result) <= 1:
    			#put filter_
				update_other_data(tabname, filter_attrib, 'int', mid_val, new_result, [low, mid_val, high, mid_val+1, high])
				low = mid_val + 1
			else:
    			#put filter_
				update_other_data(tabname, filter_attrib, 'int', mid_val, new_result, [low, mid_val, high, low, mid_val])
				high = mid_val
			cur = reveal_globals.global_conn.cursor()
			cur.execute('TRUNCATE table ' + tabname + ';')
			cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
			# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
			cur.close()
		return str(high)
	if operator == '=':
		low = min_val
		high = max_val
		flag_low = True
		flag_high = True
		#updatequery
		query =  query_front + " " + str(low) + " " + query_back + ";"
		cur = reveal_globals.global_conn.cursor()
		cur.execute(query)
		cur.close()
		new_result = executable.getExecOutput()
		# if new_result[1][0] == '0': #len(new_result) <= 1:
		if len(new_result) <= 1:
			flag_low = False
		#put filter_
		update_other_data(tabname, filter_attrib, 'int', low, new_result, [])
		query =  query_front + " " + str(high) + " " + query_back + ";"
		cur = reveal_globals.global_conn.cursor()
		cur.execute(query)
		cur.close()
		new_result = executable.getExecOutput()
		#put filter_
		update_other_data(tabname, filter_attrib, 'int', high, new_result, [])
		cur = reveal_globals.global_conn.cursor()
		cur.execute('TRUNCATE table ' + tabname + ';')
		cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
		# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
		cur.close()
		# if new_result[1][0] == '0': 
		if len(new_result) <= 1:
			flag_high = False
		return (flag_low == False and flag_high == False)
	return False

def getDateFilterValue(tabname, attrib, min_val, max_val, operator):
	counter = 0
	query_front = "update " + tabname + " set " + attrib + " = "
	query_back = ""
	firstflag = True
	cur = reveal_globals.global_conn.cursor()
	cur.execute("Truncate Table " + tabname + ";")
	cur.close()
	cur = reveal_globals.global_conn.cursor()
	cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
	# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
	cur.close()
	if operator == '<=':
		low = min_val
		high = max_val
		while int((high - low).days) > 0:
			mid_val = low + datetime.timedelta(days= int(math.ceil(((high - low).days)/2)))
			#updatequery
			query =  query_front + " '" + str(mid_val) + "' " + query_back + ";"
			cur = reveal_globals.global_conn.cursor()
			cur.execute(query)
			cur.close()
			new_result = executable.getExecOutput()
			# if new_result[1][0] == '0': 
			if len(new_result) <= 1:
				update_other_data(tabname, attrib, 'int', mid_val, new_result, [low, mid_val, high, low, mid_val- datetime.timedelta(days= 1)])
				high = mid_val - datetime.timedelta(days= 1)
			else:
				update_other_data(tabname, attrib, 'int', mid_val, new_result, [low, mid_val, high, mid_val, high])
				low = mid_val
			cur = reveal_globals.global_conn.cursor()
			cur.execute('TRUNCATE table ' + tabname + ';')
			cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
			# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
			cur.close()
		return low
	if operator == '>=':
		low = min_val
		high = max_val
		while int((high - low).days) >0:
			mid_val = low + datetime.timedelta(days= int(((high - low).days)/2))
			#updatequery
			query =  query_front + " '" + str(mid_val) + "' " + query_back + ";"
			cur = reveal_globals.global_conn.cursor()
			cur.execute(query)
			cur.close()
			new_result = executable.getExecOutput()
			# if new_result[1][0] == '0': 
			if len(new_result) <= 1:
				update_other_data(tabname, attrib, 'int', mid_val, new_result, [low, mid_val, high, mid_val + datetime.timedelta(days= 1), high])
				low = mid_val + datetime.timedelta(days= 1)
			else:
				update_other_data(tabname, attrib, 'int', mid_val, new_result, [low, mid_val, high, low, mid_val])
				high = mid_val
			cur = reveal_globals.global_conn.cursor()
			cur.execute('TRUNCATE table ' + tabname + ';')
			cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
			# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
			cur.close()
		return high
	if operator == '=':
		low = min_val
		high = max_val
		flag_low = True
		flag_high = True
		#updatequery
		query =  query_front + " '" + str(low) + "' " + query_back + ";"
		cur = reveal_globals.global_conn.cursor()
		cur.execute(query)
		cur.close()
		new_result = executable.getExecOutput()
		update_other_data(tabname, attrib, 'int', low, new_result, [])
		# if new_result[1][0] == '0': 
		if len(new_result) <= 1:
			flag_low = False
		query =  query_front + " '" + str(high) + "' " + query_back + ";"
		cur = reveal_globals.global_conn.cursor()
		cur.execute(query)
		cur.close()
		new_result = executable.getExecOutput()
		update_other_data(tabname, attrib, 'int', high, new_result, [])
		cur = reveal_globals.global_conn.cursor()
		cur.execute('TRUNCATE table ' + tabname + ';')
		cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
		# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
		cur.close()
		# if new_result[1][0] == '0': 
		if len(new_result) <= 1:
			flag_high = False
		return (flag_low == False and flag_high == False)
	return False


def checkStringPredicate(tabname, attrib):
	#updatequery
	val = ''
	if reveal_globals.global_d_plus_value[attrib] is not None and reveal_globals.global_d_plus_value[attrib][0] == 'a':
		query = "update " + tabname + " set " + attrib + " = " + "'b';"
		val = 'b'
	else:
		query = "update " + tabname + " set " + attrib + " = " + "'a';"
		val = 'a'
	cur = reveal_globals.global_conn.cursor()
	cur.execute(query)
	cur.close()
	new_result = executable.getExecOutput()
	update_other_data(tabname, attrib, 'text', val, new_result, [])
	# if new_result[1][0] == '0': #
	if len(new_result) <= 1:
		cur = reveal_globals.global_conn.cursor()
		cur.execute("Truncate Table " + tabname + ";")
		#conn.commit()
		cur.close()
		cur = reveal_globals.global_conn.cursor()
		cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
		# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
		#conn.commit()
		cur.close()
		return True
	query = "update " + tabname + " set " + attrib + " = " + "' ';"
	cur = reveal_globals.global_conn.cursor()
	cur.execute(query)
	cur.close()
	new_result = executable.getExecOutput()
	update_other_data(tabname, attrib, 'text', "''", new_result, [])
	# if new_result[1][0] == '0': 
	if len(new_result) <= 1:
		cur = reveal_globals.global_conn.cursor()
		cur.execute("Truncate Table " + tabname + ";")
		cur.close()
		cur = reveal_globals.global_conn.cursor()
		cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
		# cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
		cur.close()
		return True
	return False

def getStrFilterValue(tabname, attrib, representative, max_length):
	index = 0
	output = ""
	#currently inverted exclaimaination is being used assuming it will not be in the string
	#GET minimal string with _
	while(index < len(representative)):
		temp = list(representative)
		if temp[index] == 'a':
			temp[index] = 'b'
		else:
			temp[index] = 'a'
		temp = ''.join(temp)
		#updatequery
		query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
		cur = reveal_globals.global_conn.cursor()
		cur.execute(query)
		#conn.commit()
		cur.close()
		new_result = executable.getExecOutput()
		update_other_data(tabname, attrib, 'text', temp, new_result, [])
		# if new_result[1][0] != '0': 
		if len(new_result) > 1:
			reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[index] + "' is a replacement for wildcard character '%' or '_'"
			reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
			temp = copy.deepcopy(representative)
			temp = temp[:index] + temp[index+1:]
			#updatequery
			query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
			cur = reveal_globals.global_conn.cursor()
			cur.execute(query)
			cur.close()
			new_result = executable.getExecOutput()
			update_other_data(tabname, attrib, 'text', temp, new_result, [])
			if new_result[1][0] != '0': #len(new_result) > 1:
				reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[index] + "' is a replacement from wildcard character '%'"
				reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
				representative = representative[:index] + representative[index+1:]
			else:
				reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[index] + "' is a replacement from wildcard character '_'"
				reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
				output = output + "_"
				representative = list(representative)
				representative[index] = u"\u00A1"
				representative = ''.join(representative)
				index = index + 1
		else:
			reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[index] + "' is an intrinsic character in filter value"
			reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
			output = output + representative[index]
			index = index + 1
	if output == '':
		return output
	#GET % positions
	index = 0
	representative = copy.deepcopy(output)
	if(len(representative) < max_length):
		output = ""
		while index < len(representative):
			temp = list(representative)
			if temp[index] == 'a':
				temp.insert(index, 'b')
			else:
				temp.insert(index, 'a')
			temp = ''.join(temp)
			#updatequery
			query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
			cur = reveal_globals.global_conn.cursor()
			cur.execute(query)
			#conn.commit()
			cur.close()
			new_result = executable.getExecOutput()
			update_other_data(tabname, attrib, 'text', temp, new_result, [])
			if new_result[1][0] != '0': #len(new_result) > 1:
				output = output + '%'
			output = output + representative[index]
			index = index + 1
		temp = list(representative)
		if temp[index - 1] == 'a':
			temp.append('b')
		else:
			temp.append('a')
		temp = ''.join(temp)
		#updatequery
		query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
		cur = reveal_globals.global_conn.cursor()
		cur.execute(query)
		#conn.commit()
		cur.close()
		new_result = executable.getExecOutput()
		update_other_data(tabname, attrib, 'text', temp, new_result, [])
		if new_result[1][0] != '0': #len(new_result) > 1:
			output = output + '%'
	return output

#FILTER PREDICATES FUNCTION

def get_filter_predicates():
    reveal_globals.local_other_info_dict = {}
    reveal_globals.global_attrib_dict['filter'] = []
    get_init_data()
    filterAttribs = []
    total_attribs = 0
    attrib_types_dict = {}
    d_plus_value = copy.deepcopy(reveal_globals.global_d_plus_value)
    attrib_max_length = copy.deepcopy(reveal_globals.global_attrib_max_length)
    for entry in reveal_globals.global_attrib_types:
        attrib_types_dict[(entry[0], entry[1])] = entry[2]
    result_pos = 0
    for i in range(len(reveal_globals.global_core_relations)):
        tabname = reveal_globals.global_core_relations[i]
        attrib_list = reveal_globals.global_all_attribs[i]
        total_attribs = total_attribs + len(attrib_list)
        for attrib in attrib_list:
            #aman
            # if attrib not in reveal_globals.global_key_attributes: #comment this line
            #aman - tab start from here
            reveal_globals.global_attrib_dict['filter'].append(attrib)
            reveal_globals.local_other_info_dict = {}
            reveal_globals.local_instance_no = 1
            reveal_globals.global_instance_dict[attrib] = []
            reveal_globals.local_instance_list = []
            if 'int' in attrib_types_dict[(tabname, attrib)]:
                #NUMERIC HANDLING
                #min and max domain values (initialize based on data type)
                min_val_domain = -2147483648
                max_val_domain =  2147483647
                flag_min = checkAttribValueEffect(tabname, attrib, min_val_domain) #True implies row was still present
                flag_max = checkAttribValueEffect(tabname, attrib, max_val_domain) #True implies row was still present
                #inference based on flag_min and flag_max
                if (flag_max == True and flag_min == True):
                    reveal_globals.local_other_info_dict['Conclusion'] = 'No filter on attribute ' + attrib
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                elif (flag_min == False and flag_max == False):
                    reveal_globals.local_other_info_dict['Conclusion'] = 'Filter predicate on ' + attrib +' with operator between'
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                    print('identifying value for Int filter(range) attribute..', attrib)
                    equalto_flag = getIntFilterValue(tabname, attrib, int(d_plus_value[attrib]) - 1, int(d_plus_value[attrib]) + 1, '=')
                    if equalto_flag:
                        filterAttribs.append((tabname, attrib, '=', int(d_plus_value[attrib]), int(d_plus_value[attrib])))
                        reveal_globals.local_other_info_dict['Conclusion'] = u'Filter predicate is \u2013 ' + attrib +' = '+str(d_plus_value[attrib])
                        reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                    else:
                        val1 = getIntFilterValue(tabname, attrib, int(d_plus_value[attrib]), max_val_domain - 1, '<=')
                        val2 = getIntFilterValue(tabname, attrib, min_val_domain + 1, int(d_plus_value[attrib]), '>=')
                        filterAttribs.append((tabname, attrib, 'range', int(val2), int(val1)))
                        reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' between '+str(val2) + ' and ' + str(val1)
                        reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                elif(flag_min == True and flag_max == False):
                    reveal_globals.local_other_info_dict['Conclusion'] = 'Filter predicate on ' + attrib +' with operator <='
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                    print('identifying value for Int filter attribute', attrib)
                    val = getIntFilterValue(tabname, attrib, int(d_plus_value[attrib]), max_val_domain - 1, '<=')
                    filterAttribs.append((tabname, attrib, '<=', int(min_val_domain), int(val)))
                    reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' <= '+str(val)
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                elif(flag_min == False and flag_max == True):
                    reveal_globals.local_other_info_dict['Conclusion'] = 'Filter predicate on ' + attrib +' with operator >='
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                    print('identifying value for Int filter attribute', attrib)
                    val = getIntFilterValue(tabname, attrib, min_val_domain + 1, int(d_plus_value[attrib]), '>=')
                    filterAttribs.append((tabname, attrib, '>=', int(val), int(max_val_domain)))
                    reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' >= '+str(val)
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
            elif 'text' in attrib_types_dict[(tabname, attrib)] or 'char' in attrib_types_dict[(tabname, attrib)] or 'varbit' == attrib_types_dict[(tabname, attrib)]:
                #STRING HANDLING
                #ESCAPE CHARACTERS IN STRING REMAINING 
                if(checkStringPredicate(tabname, attrib)):
                    #returns true if there is predicate on this string attribute
                    reveal_globals.local_other_info_dict['Conclusion'] = 'Filter Predicate on '+attrib
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                    print('identifying value for String filter attribute', attrib)
                    representative = str(d_plus_value[attrib])
                    max_length = 100000
                    if(tabname, attrib) in attrib_max_length.keys():
                        max_length = attrib_max_length[(tabname, attrib)]
                    val = getStrFilterValue(tabname, attrib, representative, max_length)
                    if('%' in val or '_' in val):
                        filterAttribs.append((tabname, attrib, 'LIKE', val, val))
                        reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' LIKE '+str(val)
                    else:
                        reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' = '+str(val)
                        filterAttribs.append((tabname, attrib, 'equal', val, val))
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                else:
                    reveal_globals.local_other_info_dict['Conclusion'] = 'No Filter predicate on ' + attrib
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                #update table so that result is not empty
                cur = reveal_globals.global_conn.cursor()
                cur.execute("Truncate table " + tabname + ';')
                cur.execute(" insert into " + tabname + " select * from " + tabname + "4;")
                # cur.execute("copy " + tabname + " from " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
                cur.close()
            elif 'date' in attrib_types_dict[(tabname, attrib)]:
                #min and max domain values (initialize based on data type)
                #PLEASE CONFIRM THAT DATE FORMAT IN DATABASE IS YYYY-MM-DD
                min_val_domain = datetime.date(1,1,1)
                max_val_domain = datetime.date(9999,12,31)
                flag_min = checkAttribValueEffect(tabname, attrib, "'" + str(min_val_domain) + "'") #True implies row was still present
                flag_max = checkAttribValueEffect(tabname, attrib, "'" + str(max_val_domain) + "'") #True implies row was still present
                #inference based on flag_min and flag_max
                if (flag_max == True and flag_min == True):
                    reveal_globals.local_other_info_dict['Conclusion'] = 'No Filter predicate on ' + attrib
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                elif (flag_min == False and flag_max == False):
                    reveal_globals.local_other_info_dict['Conclusion'] = 'Filter predicate on '+attrib+' with operator between '
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                    print('identifying value for Date filter(range) attribute..', attrib)
                    equalto_flag = getDateFilterValue(tabname, attrib, d_plus_value[attrib] - datetime.timedelta(days=1), d_plus_value[attrib] + datetime.timedelta(days=1), '=')
                    if equalto_flag:
                        filterAttribs.append((tabname, attrib, '=', d_plus_value[attrib], d_plus_value[attrib]))
                        reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' = '+str(d_plus_value[attrib])
                        reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                    else:
                        val1 = getDateFilterValue(tabname, attrib, d_plus_value[attrib], max_val_domain - datetime.timedelta(days=1), '<=')
                        val2 = getDateFilterValue(tabname, attrib, min_val_domain + datetime.timedelta(days=1), d_plus_value[attrib], '>=')
                        filterAttribs.append((tabname, attrib, 'range', val2, val1))
                        reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' between '+str(val2) +' and '+str(val1)
                        reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                elif(flag_min == True and flag_max == False):
                    reveal_globals.local_other_info_dict['Conclusion'] = 'Filter predicate on ' + attrib +' with operator <='
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                    print('identifying value for Date filter attribute', attrib)
                    val = getDateFilterValue(tabname, attrib, d_plus_value[attrib], max_val_domain - datetime.timedelta(days=1), '<=')
                    filterAttribs.append((tabname, attrib, '<=', min_val_domain, val))
                    reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' <= '+str(val)
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                elif(flag_min == False and flag_max == True):
                    reveal_globals.local_other_info_dict['Conclusion'] = 'Filter predicate on ' + attrib +' with operator >= '
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                    print('identifying value for Date filter attribute', attrib)
                    val = getDateFilterValue(tabname, attrib, min_val_domain + datetime.timedelta(days=1), d_plus_value[attrib], '>=')
                    filterAttribs.append((tabname, attrib, '>=', val, max_val_domain))
                    reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' >= '+str(val)
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
            elif 'numeric' in attrib_types_dict[(tabname, attrib)]:
                #NUMERIC HANDLING
                #min and max domain values (initialize based on data type)
                min_val_domain = -214748364888
                max_val_domain =  214748364788
                #PRECISION TO BE GET FROM SCHEMA GRAPH
                precision = 2
                flag_min = checkAttribValueEffect(tabname, attrib, min_val_domain) #True implies row was still present
                flag_max = checkAttribValueEffect(tabname, attrib, max_val_domain) #True implies row was still present
                #inference based on flag_min and flag_max
                if (flag_max == True and flag_min == True):
                    reveal_globals.local_other_info_dict['Conclusion'] = 'No Filter predicate on ' + attrib
                elif (flag_min == False and flag_max == False):
                    print('identifying value for Int filter(range) attribute..', attrib)
                    equalto_flag = getIntFilterValue(tabname, attrib, float(d_plus_value[attrib]) - .01, float(d_plus_value[attrib]) + .01, '=')
                    if equalto_flag:
                        filterAttribs.append((tabname, attrib, '=', float(d_plus_value[attrib]), float(d_plus_value[attrib])))
                    else:
                        val1 = getIntFilterValue(tabname, attrib, math.ceil(float(d_plus_value[attrib])), max_val_domain, '<=')
                        val2 = getIntFilterValue(tabname, attrib, min_val_domain, math.floor(float(d_plus_value[attrib])), '>=')
                        filterAttribs.append((tabname, attrib, 'range', float(val2), float(val1)))
                elif(flag_min == True and flag_max == False):
                    print('identifying value for Int filter attribute', attrib)
                    val = getIntFilterValue(tabname, attrib, math.ceil(float(d_plus_value[attrib])), max_val_domain, '<=')
                    filterAttribs.append((tabname, attrib, '<=', float(min_val_domain), float(val)))
                elif(flag_min == False and flag_max == True):
                    print('identifying value for Int filter attribute', attrib)
                    val = getIntFilterValue(tabname, attrib, min_val_domain, math.floor(float(d_plus_value[attrib])), '>=')
                    filterAttribs.append((tabname, attrib, '>=', float(val), float(max_val_domain)))
            reveal_globals.global_instance_dict['filter_'+attrib] = copy.deepcopy(reveal_globals.local_instance_list)
            #aman - do tab till here
    pos = []
    for p in filterAttribs:
        if p[2] == '=':
            pos.append(p)
    joinAttribs = []
    vis = []
    for elt1 in pos:
        if elt1 in vis:
            continue
        if 'int' in attrib_types_dict[(elt1[0],elt1[1])]:
            val = int(elt1[3]) #for INT type
        elif 'date' in attrib_types_dict[(elt1[0],elt1[1])]:
            val = elt1[3] #datetime.strptime(prev, '%y-%m-%d') #for DATE type
        for elt2 in pos:
            if elt1 == elt2 or (elt1[3] != elt2[3]):
                continue
            if 'int' in attrib_types_dict[(elt1[0],elt1[1])]:
                cur = reveal_globals.global_conn.cursor()
                cur.execute("update " + elt1[0] + " set " + elt1[1] + " = " + str(val+1) + " ;")
                cur.execute("update " + elt2[0] + " set " + elt2[1] + " = " + str(val+1) + " ;")
                cur.close()
            elif 'date' in attrib_types_dict[(elt1[0],elt1[1])]:
                cur = reveal_globals.global_conn.cursor()
                cur.execute("update " + elt1[0] + " set " + elt1[1] + " = '" + str(val+datetime.timedelta(days= 1)) + "' ;")
                cur.execute("update " + elt2[0] + " set " + elt2[1] + " = '" + str(val+datetime.timedelta(days= 1)) + "' ;")
                cur.close()
            new_result = executable.getExecOutput()
            chk = 0 
            if new_result[1][0] != '0': #len(new_result) > 1:
                chk = 1
                joinAttribs.append((elt1[0], elt1[1], '=', elt2[0], elt2[1]))
                vis.append(elt2)
                cur = reveal_globals.global_conn.cursor()
                cur.execute("update " + elt1[0] + " set " + elt1[1] + " = " + str(val) + " ;")
                cur.execute("update " + elt2[0] + " set " + elt2[1] + " = " + str(val) + " ;")
                filterAttribs.remove(elt1)
                filterAttribs.remove(elt2)
                cur.close()
                break
            else:
                for elt3 in pos:
                    if elt1 == elt3 or elt2 == elt3 or (elt1[3] != elt3[3]):
                        continue
                    if 'int' in attrib_types_dict[(elt1[0],elt1[1])]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("update " + elt3[0] + " set " + elt3[1] + " = " + str(val+1) + " ;")
                        cur.close()
                    elif 'date' in attrib_types_dict[(elt1[0],elt1[1])]:
                        cur = reveal_globals.global_conn.cursor()
                        cur.execute("update " + elt3[0] + " set " + elt3[1] + " = '" + str(val+datetime.timedelta(days= 1)) + "' ;")
                        cur.close()
                    new_result = executable.getExecOutput()
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("update " + elt3[0] + " set " + elt3[1] + " = " + str(val) + " ;")
                    cur.close()
                    if new_result[1][0] != '0': #len(new_result) > 1:
                        chk = 1
                        joinAttribs.append((elt1[0], elt1[1], '=', elt2[0], elt2[1]))
                        joinAttribs.append((elt1[0], elt1[1], '=', elt3[0], elt3[1]))
                        vis.append(elt2)
                        vis.append(elt3)
                        filterAttribs.remove(elt1)
                        filterAttribs.remove(elt2)
                        filterAttribs.remove(elt3)
                        break
                if chk == 1:
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("update " + elt1[0] + " set " + elt1[1] + " = " + str(val) + " ;")
                    cur.execute("update " + elt2[0] + " set " + elt2[1] + " = " + str(val) + " ;")
                    cur.close()
                    break
            cur = reveal_globals.global_conn.cursor()
            cur.execute("update " + elt1[0] + " set " + elt1[1] + " = " + str(val+1) + " ;")
            cur.execute("update " + elt2[0] + " set " + elt2[1] + " = " + str(val+1) + " ;")
            cur.close()
    print("Join: ", joinAttribs)
    return filterAttribs 