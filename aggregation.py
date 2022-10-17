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


def getKValue(a, b):
	k_value = 0
	agg_array = []
	constraint_array = []
	if (a == b):
		k_value = 1
		if a == 2:
			k_value = 2
		agg_array = ['Sum', k_value*a + b, 'Avg', a, 'Min', a, 'Max', a, 'Count', k_value + 1]
	else:
		constraint_array = [0, a, b, a-1, b-1]
		if(a != 0):
			constraint_array.append((a - b)/a)
		if(a != 1):
			constraint_array.append((1 - b)/(a - 1))
		if((a - 2)**2 - (4 * (1 - b)) >= 0):
			constraint_array.append(((a - 2) + math.sqrt((a - 2)**2 - (4 * (1 - b))))/2)
		k_value = 2
		while(k_value in constraint_array):
			k_value = k_value + 1
		avg = round((k_value*a + b)/(k_value + 1), 2)
		if (avg/int(avg) == 1):
			avg = int(avg)
		agg_array = ['Sum', k_value*a + b, 'Avg', avg, 'Min', min(a, b), 'Max', max(a, b), 'Count', k_value + 1]
	return k_value, agg_array


def get_aggregation():
	print("inside aggregation.get_aggregation")
	#Assuming NO DISTINCT IN AGGREGATION
	attrib_types_dict = {}
	reveal_globals.global_attrib_dict['agg'] = []
	reveal_globals.local_other_info_dict = {}
	for entry in reveal_globals.global_attrib_types:
		attrib_types_dict[(entry[0], entry[1])] = entry[2]
	AggregationList = [(element, '') for element in reveal_globals.global_projected_attributes]
	if reveal_globals.global_groupby_flag == False:
		return AggregationList
	dummy_int = 2
	dummy_char = 65
	dummy_date = datetime.date(1000,1,1)
	filter_attrib_dict = {}
	#get filter values and their allowed minimum and maximum value
	for entry in reveal_globals.global_filter_predicates:
		if len(entry) > 4 and 'like' not in entry[2].lower() and 'equal' not in entry[2].lower():
			filter_attrib_dict[(entry[0], entry[1])] = (entry[3], entry[4])
		else:
			filter_attrib_dict[(entry[0], entry[1])] = entry[3]
	for i in range(len(reveal_globals.global_core_relations)):
		tabname = reveal_globals.global_core_relations[i]
		attrib_list = copy.deepcopy(reveal_globals.global_all_attribs[i])
		for attrib in attrib_list:
			result_index = 0
			#check if it is a key attribute
			key_list = []
			groupby_key_flag = False
			for elt in reveal_globals.global_join_graph:
				if attrib in elt:
					key_list = elt
					break
			#Attribute Filtering
			if attrib not in reveal_globals.global_projected_attributes or (attrib in reveal_globals.global_groupby_attributes):
				continue
			else:
				result_index_list = [j for j, x in enumerate(reveal_globals.global_projected_attributes) if x == attrib]
			if attrib in reveal_globals.global_key_attributes and attrib in reveal_globals.global_groupby_attributes:
				groupby_key_flag = True
			for result_index in result_index_list:
				reveal_globals.global_attrib_dict['agg'].append(attrib)
				reveal_globals.local_other_info_dict = {}
				k_value = -1
				a = 0
				b = 0
				agg_array = [] # [Sum, Avg, Min, Max, Count]
				if groupby_key_flag == True and ('int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[(tabname, attrib)]):
					a = b = 3
					k_value = 1
					agg_array = ['Sum', k_value*a + b, 'Avg', a, 'Min', a, 'Max', a, 'Count', k_value + 1]
				elif (tabname, attrib) in filter_attrib_dict.keys() and ('int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[(tabname, attrib)]):
					#PRECISION TO BE TAKEN CARE FOR NUMERIC
					a = filter_attrib_dict[(tabname, attrib)][0]
					b = min(filter_attrib_dict[(tabname, attrib)][0] + 1, filter_attrib_dict[(tabname, attrib)][1])
					if(a == 0): #swap a and b
						a = b
						b = 0
					k_value, agg_array = getKValue(a, b)
				elif (tabname, attrib) in filter_attrib_dict.keys() and 'date' in attrib_types_dict[(tabname, attrib)]:
					a = str(filter_attrib_dict[(tabname, attrib)][0])
					b = str(min(filter_attrib_dict[(tabname, attrib)][0] + datetime.timedelta(days=1), filter_attrib_dict[(tabname, attrib)][1]))
					k_value = 1
					agg_array = ['Min', min(a, b), 'Max', max(a, b)]
					a = "'" + a + "'"
					b = "'" + b + "'"
				elif (tabname, attrib) in filter_attrib_dict.keys():
					#string filter attribute
					if '_' in filter_attrib_dict[(tabname, attrib)]:
						a = filter_attrib_dict[(tabname, attrib)].replace('_', 'a')
						b = filter_attrib_dict[(tabname, attrib)].replace('_', 'b')
					else:
						a = filter_attrib_dict[(tabname, attrib)].replace('%', 'a', 1)
						b = filter_attrib_dict[(tabname, attrib)].replace('%', 'b', 1)
					a = a.replace('%', '')
					b = b.replace('%', '')
					k_value = 1
					agg_array = ['Min', min(a, b), 'Max', max(a, b)]
				elif 'date' in attrib_types_dict[(tabname, attrib)]:
					a = str(dummy_date)
					b = str(dummy_date + datetime.timedelta(days=1))
					k_value = 1
					agg_array = ['Min', min(a, b), 'Max', max(a, b)]
				elif ('int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[(tabname, attrib)]):
					#Combination which gives all different results for aggregation
					a = 5
					b = 8
					k_value = 2
					agg_array = ['Sum', 18, 'Avg', 6, 'Min', 5, 'Max', 8, 'Count', 3]
				else:
					#String data type
					a = chr(dummy_char)
					b = chr(dummy_char + 1)
					k_value = 1
					agg_array = ['Min', min(a, b), 'Max', max(a, b)]
				#Truncate all core relations
				for table in reveal_globals.global_core_relations:
					cur = reveal_globals.global_conn.cursor()
					cur.execute('Truncate Table ' + table + ';')
					cur.close()
				#For this table (tabname) and this attribute (attrib), fill all tables now
				for j in range(len(reveal_globals.global_core_relations)):
					tabname_inner = reveal_globals.global_core_relations[j]
					attrib_list_inner = reveal_globals.global_all_attribs[j]
					insert_rows = []
					no_of_rows = k_value + 1
					key_path_flag = False
					if tabname_inner != tabname:
						no_of_rows = 1
					for val in attrib_list_inner:
						if val in key_list:
							key_path_flag = True
							break
					if tabname_inner != tabname and key_path_flag == True:
						no_of_rows = 2
					for k in range(no_of_rows):
						insert_values = []
						att_order = '('
						flag = 0
						for attrib_inner in attrib_list_inner:
							if flag == 0:
								att_order += attrib_inner+","
							if (attrib_inner == attrib or attrib_inner in key_list) and k == no_of_rows - 1:
								insert_values.append(b)
							elif (attrib_inner == attrib or attrib_inner in key_list):
								insert_values.append(a)
							elif 'date' in attrib_types_dict[(tabname_inner, attrib_inner)]:
								#check for filter
								if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
									insert_values.append("'" + str(filter_attrib_dict[(tabname_inner, attrib_inner)][0]) + "'")
								else:
									insert_values.append("'" + str(dummy_date + datetime.timedelta(days=2)) + "'")
							elif 'int' in attrib_types_dict[(tabname_inner, attrib_inner)] or 'numeric' in attrib_types_dict[(tabname_inner, attrib_inner)]:
								#check for filter
								if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
									insert_values.append(filter_attrib_dict[(tabname_inner, attrib_inner)][0])
								else:
									insert_values.append(dummy_int)
							else:
								#check for filter
								if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
									insert_values.append(filter_attrib_dict[(tabname_inner, attrib_inner)].replace('%', ''))
								else:
									insert_values.append(chr(dummy_char + 2))
						insert_rows.append(tuple(insert_values))
						flag = 1
					esc_string = '(' + '%s'
					for k in range(1, len(attrib_list_inner)):
						esc_string = esc_string + ", " + '%s'
					esc_string = esc_string + ")"
					att_order=att_order[:-1]
					att_order+=')'
					cur = reveal_globals.global_conn.cursor()
					for x in insert_rows:
						query="INSERT INTO " + tabname_inner + att_order + " VALUES "+ esc_string
						cur.execute(query, x)
					#insert multiple rows together
					#cur.execute("INSERT INTO " + tabname_inner + " VALUES " + esc_string, insert_rows)
					# args_str = ','.join(cur.mogrify(esc_string, x).decode("utf-8") for x in insert_rows)
					# cur.execute("INSERT INTO " + tabname_inner + att_order+ " VALUES " + args_str)
					reveal_globals.global_min_instance_dict['agg_' + attrib + '_' + tabname_inner +'_D_gen1'] = copy.deepcopy(insert_rows)
					reveal_globals.global_min_instance_dict['agg_' + attrib + '_' + tabname_inner +'_D_gen1'].insert(0, copy.deepcopy(reveal_globals.global_min_instance_dict[tabname_inner][0]))
					reveal_globals.local_other_info_dict['Table ' + tabname_inner] = str(len(insert_rows)) + ' row(s)'
					cur.close()
				new_result = executable.getExecOutput()
				reveal_globals.local_other_info_dict['Possible Aggregation values for ' + attrib] = "\n                    " + str(agg_array)
				reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1) + ' row(s)'
				reveal_globals.global_result_dict['agg_' + attrib + '_D_gen1'] = copy.deepcopy(new_result)
				if len(new_result) <= 1:
					reveal_globals.error="Unmasque: \n some error in generating new database. Result is empty. Can not identify aggregation"
					print('some error in generating new database. Result is empty. Can not identify aggregation')
					# exit(1)
					return []
				if len(new_result) > 2:
					continue
				new_result = list(new_result[1])
				new_result = [x.strip() for x in new_result]
				check_value = 0
				if(is_number(new_result[result_index])):
					check_value = round(float(new_result[result_index]), 2)
					if(check_value / int(check_value) == 1):
						check_value = int(check_value)
				else:
					check_value = str(new_result[result_index])
				j = 0
				while(j<len(agg_array) - 1):
					#print(agg_array[j+1], type(agg_array[j+1]))
					if(check_value == agg_array[j+1]):
						AggregationList[result_index] = (str(attrib),agg_array[j])
						break
					j = j + 2
				reveal_globals.local_other_info_dict['Actual Aggregation value for ' + attrib+' in the result'] = str(check_value)
				reveal_globals.local_other_info_dict['Conclusion'] = 'Aggregation on ' + attrib + ' is ' + AggregationList[result_index][1]
				reveal_globals.global_other_info_dict['agg_' +attrib+'_D_gen1'] = copy.deepcopy(reveal_globals.local_other_info_dict)
	for i in range(len(reveal_globals.global_projected_attributes)):
		if(reveal_globals.global_projected_attributes[i] == ''):
			AggregationList[i] = ('','count(*)')
	return AggregationList