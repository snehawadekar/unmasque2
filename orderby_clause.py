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

class CandidateAttribute():
	"""docstring for CandidateAttribute"""
	def __init__(self, attrib, aggregation, dependency, dependencyList, index, name):
		self.attrib = attrib
		self.aggregation = aggregation
		self.dependency = dependency
		self.dependencyList = copy.deepcopy(dependencyList)
		self.index = index
		self.name = name

	def print(self):
		print(self.attrib)
		print(self.aggregation)
		print(self.dependency)
		print(self.dependencyList)
		print(self.index)
		print('')

def checkOrdering(obj, result):
	print("inside -- orderby_clause.checkOrdering")
	# print(len(result),result)
	# time.sleep(30)
	try:
		i=2
		got_ans=0
		while(not(got_ans)):
			if result[1][obj.index] > result[i][obj.index]:
				ans= 'desc'
				got_ans=1
			elif result[1][obj.index] < result[i][obj.index]:
				ans= 'asc'
				got_ans=1
			i+=1
		return ans
	except:
		return None

def generateData(obj, orderby_list, filter_attrib_dict, curr_orderby):
	print("inside -- orderby_clause.generateData")
	reveal_globals.local_other_info_dict = {}
	dummy_int = 2
	dummy_char = 65 # to avoid having space/tab
	dummy_date = datetime.date(1000,1,1)
	attrib_types_dict = {}
	for entry in reveal_globals.global_attrib_types:
		attrib_types_dict[(entry[0], entry[1])] = entry[2]
	#check if it is a key attribute, #NO CHECKING ON KEY ATTRIBUTES
	if obj.attrib in reveal_globals.global_key_attributes:
		return None
	if obj.dependency == False:
		if obj.name not in reveal_globals.global_attrib_dict['order by']:
			reveal_globals.global_attrib_dict['order by'].append(obj.name)
		#ATTRIBUTES TO GET SAME VALUE FOR BOTH ROWS
		#EASY AS KEY ATTRIBUTES ARE NOT THERE IN ORDER AS PER ASSUMPTION SO FAR
		#IN CASE OF COUNT ---
		#Fill 3 rows in any one table (with a a b values) and 2 in all others (with a b values) in D1
		#Fill 3 rows in any one table (with a b b values) and 2 in all others (with a b values) in D2	
		same_value_list = []
		for elt in orderby_list:
			same_value_list.append(elt[0].attrib)
		no_of_db = 2
		order = [None, None]
		#For this attribute (obj.attrib), fill all tables now
		for k in range(no_of_db):
			#Truncate all core relations
			for table in reveal_globals.global_core_relations:
				cur = reveal_globals.global_conn.cursor()
				cur.execute('Truncate Table ' + table + ';')
				##conn.commit()
				cur.close()
			for j in range(len(reveal_globals.global_core_relations)):
				first_rel = reveal_globals.global_core_relations[0]
				tabname_inner = reveal_globals.global_core_relations[j]
				attrib_list_inner = reveal_globals.global_all_attribs[j]
				insert_rows = []
				insert_values1 = []
				insert_values2 = []
				att_order = '('
				flag = 0
				for attrib_inner in attrib_list_inner:
					first = ''
					second = ''
					if(flag == 0):
						att_order += attrib_inner + ","
					if 'date' in attrib_types_dict[(tabname_inner, attrib_inner)]:
						if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
							first = filter_attrib_dict[(tabname_inner, attrib_inner)][0]
							second = min(filter_attrib_dict[(tabname_inner, attrib_inner)][0] + datetime.timedelta(days=1), filter_attrib_dict[(tabname_inner, attrib_inner)][1])
						else:
							first = dummy_date
							second = dummy_date + datetime.timedelta(days=1)
						insert_values1.append("'" + str(first) + "'")
						insert_values2.append("'" + str(second) + "'")
					elif ('int' in attrib_types_dict[(tabname_inner, attrib_inner)] or 'numeric' in attrib_types_dict[(tabname_inner, attrib_inner)]):
						#check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
						if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
							first = filter_attrib_dict[(tabname_inner, attrib_inner)][0]
							second = min(filter_attrib_dict[(tabname_inner, attrib_inner)][0] +1, filter_attrib_dict[(tabname_inner, attrib_inner)][1])
						else:
							first = dummy_int
							second = dummy_int + 1
						insert_values1.append(first)
						insert_values2.append(second)
					else:
						if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
							#EQUAL FILTER WILL NOT COME HERE
							if '_' in filter_attrib_dict[(tabname_inner, attrib_inner)]:
								string = copy.deepcopy(filter_attrib_dict[(tabname_inner, attrib_inner)])
								first  = string.replace('_', chr(dummy_char))
								string = copy.deepcopy(filter_attrib_dict[(tabname_inner, attrib_inner)])
								second = string.replace('_', chr(dummy_char + 1))
							else:
								string = copy.deepcopy(filter_attrib_dict[(tabname_inner, attrib_inner)])
								first  = string.replace('%', chr(dummy_char), 1)
								string = copy.deepcopy(filter_attrib_dict[(tabname_inner, attrib_inner)])
								second = string.replace('%', chr(dummy_char + 1), 1)
							first.replace('%', '')
							second.replace('%', '')
						else:
							first = chr(dummy_char)
							second = chr(dummy_char + 1)
						insert_values1.append(first)
						insert_values2.append(second)
					if k == no_of_db - 1 and (attrib_inner == obj.attrib or 'count' in obj.aggregation):
						#swap first and second
						insert_values2[-1], insert_values1[-1] = insert_values1[-1], insert_values2[-1]
					if attrib_inner in same_value_list:
						insert_values2[-1] = insert_values1[-1]
				flag = 1
				if 'count' in obj.aggregation and tabname_inner == first_rel:
					insert_rows.append(tuple(insert_values1))
					insert_rows.append(tuple(insert_values1))
					insert_rows.append(tuple(insert_values2))
				else:
					insert_rows.append(tuple(insert_values1))
					insert_rows.append(tuple(insert_values2))
				esc_string = '(' + '%s'
				for temp in range(1, len(attrib_list_inner)):
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
				# cur.execute("INSERT INTO " + tabname_inner + att_order + " VALUES " + args_str)
				cur.close()
				reveal_globals.global_min_instance_dict['order by_' + obj.name +'_'+ tabname_inner + '_D_gen' + str(k+1)] = copy.deepcopy(insert_rows)
				reveal_globals.global_min_instance_dict['order by_' + obj.name +'_'+ tabname_inner + '_D_gen' + str(k+1)].insert(0, copy.deepcopy(reveal_globals.global_min_instance_dict[tabname_inner][0]))
				reveal_globals.local_other_info_dict['Table ' + tabname_inner] = str(len(insert_rows)) + ' row(s)'
			new_result = executable.getExecOutput()
			reveal_globals.global_result_dict['order by_' + obj.name + '_D_gen' + str(k+1)] = copy.deepcopy(new_result)
			reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1) + ' row(s)'
			if len(new_result) <= 1:
				reveal_globals.error="Unmasque:\n some error in generating new database. Result is empty. Can not identify Ordering"
				print('some error in generating new database. Result is empty. Can not identify Ordering')
				return None
			if len(new_result) == 2:
				return None
			order[k] = checkOrdering(obj, new_result)
			reveal_globals.local_other_info_dict['Ordering of ' + obj.name + ' in the current result'] = order[k]
			reveal_globals.global_other_info_dict['order by_' + obj.name + '_D_gen' + str(k+1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
			reveal_globals.global_extracted_info_dict['order by_'+ obj.name + '_D_gen' + str(k+1)] = copy.deepcopy(reveal_globals.global_extracted_info_dict['agg'])
			reveal_globals.global_extracted_info_dict['order by_'+ obj.name + '_D_gen' + str(k+1)]['ORDER BY'] = curr_orderby
		if order[0] != None and order[1] != None and order[0] == order[1]:
			reveal_globals.local_other_info_dict['Conclusion'] = order[0] + ' ordering on column ' + obj.name
			reveal_globals.global_other_info_dict['order by_' + obj.name + '_D_gen' + str(2)] = copy.deepcopy(reveal_globals.local_other_info_dict)
			return order[0]
		else:
			reveal_globals.local_other_info_dict['Conclusion'] = 'No ordering on column ' + obj.name
			reveal_globals.global_other_info_dict['order by_' + obj.name + '_D_gen' + str(2)] = copy.deepcopy(reveal_globals.local_other_info_dict)
			return 'noorder'
	return 'noorder'

def get_orderby_attributes():
	print("inside -- orderby_clause.get_orderby_attributes")
	#ORDERBY ON PROJECTED COLUMNS ONLY
	#ASSUMING NO ORDER ON JOIN ATTRIBUTES
	agg_dict = {}
	orderby_list = []
	cand_list = []
	reveal_globals.global_attrib_dict['order by'] = []
	for i in range(len(reveal_globals.global_aggregated_attributes)):
		dependencyList = []
		for j in range(len(reveal_globals.global_aggregated_attributes)):
			if j != i and reveal_globals.global_aggregated_attributes[i][0] == reveal_globals.global_aggregated_attributes[j][0]:
				dependencyList.append((reveal_globals.global_aggregated_attributes[j]))
		cand_list.append(CandidateAttribute(reveal_globals.global_aggregated_attributes[i][0], reveal_globals.global_aggregated_attributes[i][1], not(not dependencyList), dependencyList, i, reveal_globals.global_projection_names[i]))
	filter_attrib_dict = {}
	#get filter values and their allowed minimum and maximum value
	for entry in reveal_globals.global_filter_predicates:
		if len(entry) > 4 and 'like' not in entry[2].lower() and 'equal' not in entry[2].lower():
			filter_attrib_dict[(entry[0], entry[1])] = (entry[3], entry[4])
		else:
			filter_attrib_dict[(entry[0], entry[1])] = entry[3]
	#REMOVE ELEMENTS WITH EQUALITY FILTER PREDICATES
	remove_list = []
	for elt in cand_list:
		for entry in reveal_globals.global_filter_predicates:
			if elt.attrib == entry[1] and (entry[2] == '=' or entry[2] == 'equal') and not ('sum' in elt.aggregation or 'count' in elt.aggregation):
				remove_list.append(elt)
	for elt in remove_list:
		cand_list.remove(elt)
	order_found_flag = True
	curr_orderby = ""
	while order_found_flag == True and not(not cand_list):
		remove_list = []
		order_found_flag = False
		for elt in cand_list:
			if 'count' in elt.aggregation:
				continue
			order = generateData(elt, orderby_list, filter_attrib_dict, curr_orderby)
			if order == None:
				remove_list.append(elt)
			elif order != 'noorder':
				order_found_flag = True
				orderby_list.append((elt, order))
				curr_orderby += elt.name + " " + order + ", "
				remove_list.append(elt)
				break
		for elt in remove_list:
			cand_list.remove(elt)
	#CHECK ORDER BY ON COUNT
	for elt in cand_list:
		if 'count' not in elt.aggregation:
			continue
		for i in range(len(orderby_list) + 1):
			temp_orderby_list = []
			for j in range(i):
				temp_orderby_list.append(orderby_list[j])
			order = generateData(elt, temp_orderby_list, filter_attrib_dict, curr_orderby)
			if order == None:
				break
			elif order != 'noorder' and order != None:
				orderby_list.insert(i, (elt, order))
				curr_orderby +=  "count(*) " + order + ", "
				break
	#hardcoding for demo
	if reveal_globals.global_input_type == "1":
		reveal_globals.global_attrib_dict['order by'].sort()
	#####################
	print(orderby_list)
	return orderby_list