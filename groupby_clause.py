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
import itertools
import executable


def is_number(s):
	try:
		float(s)
		return True
	except ValueError:
		return False

def is_int(s):
	try:
		int(s)
		return True
	except ValueError:
		return False

def getGroupByAttributes_disj():
	possible_filter_pred = []
	fp_disj = copy.deepcopy(reveal_globals.global_filter_predicates_disj)
	# while len(fp_disj) != 0:
	somelist=[]
	for i in range(len(fp_disj)):
		somelist.append(fp_disj[i])
		
	ps = itertools.product(*somelist)
	for element in ps:
		print("-------------")
		print(list(element))
		possible_filter_pred.append(list(element))


	
   
	for k in range(len(possible_filter_pred)):
		reveal_globals.global_filter_predicates = possible_filter_pred[k]
		print(reveal_globals.global_filter_predicates)
		# reveal_globals.global_groupby_attributes, reveal_globals.global_groupby_flag = getGroupByAttributes()
		groupby_temp, groupby_flag = getGroupByAttributes()
		if groupby_flag:
			reveal_globals.global_groupby_flag = True
		for ele in groupby_temp:
			if ele not in reveal_globals.global_groupby_attributes:
				reveal_globals.global_groupby_attributes.append(ele)

	print(reveal_globals.global_groupby_attributes)
  
    
        

def getGroupByAttributes():
	print("inside ---groupby_clause.getGroupByAttributes")
	attrib_types_dict = {}
	reveal_globals.global_attrib_dict['group by'] = []
	for entry in reveal_globals.global_attrib_types:
		attrib_types_dict[(entry[0], entry[1])] = entry[2]
	groupbyflag = False
	dummy_int = 2
	dummy_char = 65 # to avoid having space/tab
	dummy_date = datetime.date(1000,1,1)
	GroupByAttrib = []
	filter_attrib_dict = {}
	#get filter values and their allowed minimum and maximum value
	for entry in reveal_globals.global_filter_predicates:
		if len(entry) > 4 and 'like' not in entry[2].lower() and 'equal' not in entry[2].lower():
			filter_attrib_dict[(entry[0], entry[1])] = (entry[3], entry[4])
		else:
			filter_attrib_dict[(entry[0], entry[1])] = entry[3]
	for i in range(len(reveal_globals.global_core_relations)):
		tabname = reveal_globals.global_core_relations[i]
		attrib_list = reveal_globals.global_all_attribs[i]
		for attrib in attrib_list:
			reveal_globals.local_other_info_dict = {}
			reveal_globals.global_attrib_dict['group by'].append(attrib)
			#Truncate all core relations
			for table in reveal_globals.global_core_relations:
				cur = reveal_globals.global_conn.cursor()
				cur.execute('Truncate Table ' + table + ';')
				##conn.commit()
				cur.close()
			#determine offset values for this attribute
			curr_attrib_value = [0, 1, 1]
			#check if it is a key attribute
			key_list = []
			for elt in reveal_globals.global_join_graph:
				if attrib in elt:
					key_list = elt
					break
			#For this table (tabname) and this attribute (attrib), fill all tables now
			for j in range(len(reveal_globals.global_core_relations)):
				tabname_inner = reveal_globals.global_core_relations[j]
				attrib_list_inner = reveal_globals.global_all_attribs[j]
				insert_rows = []
				key_path_flag = False
				no_of_rows = 3
				if tabname_inner != tabname:
					no_of_rows = 1
				for val in attrib_list_inner:
					if val in key_list:
						key_path_flag = True
						break
				if tabname_inner != tabname and key_path_flag == True:
					no_of_rows = 2
				att_order = '('	
				flag = 0
				for k in range(no_of_rows):
					insert_values = []
					for attrib_inner in attrib_list_inner:
						if flag == 0:
    							att_order += attrib_inner+","		
						if 'date' in attrib_types_dict[(tabname_inner, attrib_inner)] and (attrib_inner == attrib or attrib_inner in key_list):
							if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
								insert_values.append("'" + str(min(filter_attrib_dict[(tabname_inner, attrib_inner)][0] + datetime.timedelta(days=curr_attrib_value[k]), filter_attrib_dict[(tabname_inner, attrib_inner)][1])) + "'")
							else:
								insert_values.append("'" + str(dummy_date + datetime.timedelta(days=curr_attrib_value[k])) + "'")
						elif ('int' in attrib_types_dict[(tabname_inner, attrib_inner)] or 'numeric' in attrib_types_dict[(tabname_inner, attrib_inner)]) and (attrib_inner == attrib or attrib_inner in key_list):
							#check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
							if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
								insert_values.append(min(filter_attrib_dict[(tabname_inner, attrib_inner)][0] + curr_attrib_value[k], filter_attrib_dict[(tabname_inner, attrib_inner)][1]))
							else:
								insert_values.append(dummy_int + curr_attrib_value[k])
						elif (attrib_inner == attrib or attrib_inner in key_list):
							if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
								if '_' in filter_attrib_dict[(tabname_inner, attrib_inner)]:
									insert_values.append(filter_attrib_dict[(tabname_inner, attrib_inner)].replace('_', chr(dummy_char + curr_attrib_value[k])))
								else:
									insert_values.append(filter_attrib_dict[(tabname_inner, attrib_inner)].replace('%', chr(dummy_char + curr_attrib_value[k]), 1))
								insert_values[-1].replace('%', '')
							else:
								insert_values.append(chr(dummy_char + curr_attrib_value[k]))
						elif 'date' in attrib_types_dict[(tabname_inner, attrib_inner)]:
							if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
								insert_values.append("'" + str(filter_attrib_dict[(tabname_inner, attrib_inner)][0]) +  "'")
							else:
								insert_values.append("'" + str(dummy_date) + "'")
						elif 'int' in attrib_types_dict[(tabname_inner, attrib_inner)] or 'numeric' in attrib_types_dict[(tabname_inner, attrib_inner)]:
							#check for filter
							if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
								insert_values.append(filter_attrib_dict[(tabname_inner, attrib_inner)][0])
							else:
								insert_values.append(dummy_int)
						else:
							if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
								insert_values.append(filter_attrib_dict[(tabname_inner, attrib_inner)].replace('%', ''))
							else:
								insert_values.append(chr(dummy_char))
					flag = 1
					insert_rows.append(tuple(insert_values))
				esc_string = '(' + '%s'
				for k in range(1, len(attrib_list_inner)):
					esc_string = esc_string + ", " + '%s'
				esc_string = esc_string + ")"
				att_order=att_order[:-1]
				att_order+=')'
				cur = reveal_globals.global_conn.cursor()
				#insert multiple rows together
				#cur.execute("INSERT INTO " + tabname_inner + " VALUES " + esc_string, insert_rows)
				for x in insert_rows:
					query="INSERT INTO " + tabname_inner + att_order + " VALUES "+ esc_string
					cur.execute(query, x)
				# print("insert_rows=", insert_rows)
				# print("esc_string=", esc_string)
				# print("att_order=", att_order)
				# args_str = ','.join(cur.mogrify(esc_string, x).decode("utf-8") for x in insert_rows)
				# cur.execute("INSERT INTO " + tabname_inner + att_order + " VALUES " + args_str)
				reveal_globals.global_min_instance_dict['group by_' + attrib + '_' + tabname_inner +'_D_gen1'] = copy.deepcopy(insert_rows)
				reveal_globals.global_min_instance_dict['group by_' + attrib + '_' + tabname_inner +'_D_gen1'].insert(0, copy.deepcopy(reveal_globals.global_min_instance_dict[tabname_inner][0]))
				reveal_globals.local_other_info_dict['Table ' + tabname_inner] = str(len(insert_rows)) + ' row(s)'
				#conn.commit()
				cur.close()
			new_result = executable.getExecOutput()
			reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)+' rows'
			reveal_globals.global_result_dict['group by_' + attrib + '_D_gen1'] = copy.deepcopy(new_result)
			if len(new_result) <= 1:
				reveal_globals.error="Unmasque Error :\n Some error in generating new database. Result is empty. Can not identify Grouping"
				print('some error in generating new database. Result is empty. Can not identify Grouping')
				return [], False
			if len(new_result) == 3:
				#3 is WITH HEADER so it is checking for two rows
				GroupByAttrib.append(attrib)
				reveal_globals.local_other_info_dict['Conclusion'] = 'Grouping on attribute ' + attrib
				groupbyflag = True
			if len(new_result) == 2:
				#It indicates groupby on at least one attribute
				groupbyflag = True
				reveal_globals.local_other_info_dict['Conclusion'] = ' No Grouping on attribute ' + attrib
			reveal_globals.global_other_info_dict['group by_' +attrib+'_D_gen1'] = copy.deepcopy(reveal_globals.local_other_info_dict)
	return GroupByAttrib, groupbyflag