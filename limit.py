import sys


import reveal_globals
import datetime
import copy
import math
import executable
import itertools


	
def get_limit():
	print("inside -- limit.get_limit")
	reveal_globals.local_other_info_dict = {}
	pre_assignment = True
	grouping_attribute_values = {}
	filter_attrib_dict = {}
	filter_onlyattrib_dict = {}
	attrib_types_dict = {}
	attribonly_types_dict = {}
	for entry in reveal_globals.global_attrib_types:
		attrib_types_dict[(entry[0], entry[1])] = entry[2]
		attribonly_types_dict[entry[1]] = entry[2]
	for elt in reveal_globals.global_groupby_attributes:
		if elt in reveal_globals.global_key_attributes:
			pre_assignment = False
			break
	if reveal_globals.global_groupby_attributes == []:
		pre_assignment = False
	#get filter values and their allowed minimum and maximum value
	for entry in reveal_globals.global_filter_predicates:
		if len(entry) > 4 and 'like' not in entry[2].lower() and 'equal' not in entry[2].lower():
			filter_attrib_dict[(entry[0], entry[1])] = (entry[3], entry[4])
			filter_onlyattrib_dict[entry[1]] = (entry[3], entry[4])
		else:
			filter_attrib_dict[(entry[0], entry[1])] = entry[3]
			filter_onlyattrib_dict[entry[1]] = entry[3]
	total_combinations = 1
	if pre_assignment == True:
		#GET LIMITS FOR ALL GROUPBY ATTRIBUTES
		group_lists = []
		for elt in reveal_globals.global_groupby_attributes:
			temp = []
			if elt not in filter_onlyattrib_dict.keys():
				pre_assignment = False
				break
			if ('int' in attribonly_types_dict[elt] or 'numeric' in attribonly_types_dict[elt] or 'date' in attribonly_types_dict[elt]):
				if 'date' in attribonly_types_dict[elt]:
					tot_values = (filter_onlyattrib_dict[elt][1] - filter_onlyattrib_dict[elt][0]).days + 1
				else:
					tot_values = filter_onlyattrib_dict[elt][1] - filter_onlyattrib_dict[elt][0] + 1
				if (total_combinations * tot_values) > 1000:
					i = 1
					while (total_combinations * i) < 1001 and i < tot_values:
						i = i + 1
					tot_values = i
				if 'date' in attribonly_types_dict[elt]:
					for k in range(tot_values):
						temp.append(str("'" + str(filter_onlyattrib_dict[elt][0] + datetime.timedelta(days = k)) + "'"))
				else:
					for k in range(tot_values):
						temp.append(filter_onlyattrib_dict[elt][0] + k)
			else:
				if '%' in filter_onlyattrib_dict[elt] or '_' in filter_onlyattrib_dict[elt]:
					pre_assignment = False
					break
				else:
					temp = [filter_onlyattrib_dict[elt]]
			total_combinations = total_combinations * len(temp)
			group_lists.append(copy.deepcopy(temp))
		#CREATE DIFFERENT PERMUTATIONS OF GROUPBY COLUMNS VALUE ASSIGNMENTS HERE
		if pre_assignment == True:
			combo_values = list(itertools.product(*group_lists))
			for elt in reveal_globals.global_groupby_attributes:
				grouping_attribute_values[elt] = []
			for elt in combo_values:
				temp = list(elt)
				for (val1, val2) in zip(reveal_globals.global_groupby_attributes, temp):
					grouping_attribute_values[val1].append(val2)
	no_of_rows = 1000
	if pre_assignment == True:
		no_of_rows = min(no_of_rows, total_combinations)
	dummy_int = 2
	dummy_char = 65 # to avoid having space/tab
	dummy_date = datetime.date(1000,1,1)
	for j in range(len(reveal_globals.global_core_relations)):
		tabname_inner = reveal_globals.global_core_relations[j]
		attrib_list_inner = reveal_globals.global_all_attribs[j]
		att_order = '('	
		flag = 0
		insert_rows = []
		for k in range(no_of_rows):
			insert_values = []
			for attrib_inner in attrib_list_inner:
				if flag == 0:
					att_order += attrib_inner+","
				if attrib_inner in grouping_attribute_values.keys():
					insert_values.append(grouping_attribute_values[attrib_inner][k])
				elif 'date' in attrib_types_dict[(tabname_inner, attrib_inner)]:
					if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
						insert_values.append("'" + str(min(filter_attrib_dict[(tabname_inner, attrib_inner)][0] + datetime.timedelta(days = k), filter_attrib_dict[(tabname_inner, attrib_inner)][1])) + "'")
					else:
						insert_values.append("'" + str(dummy_date + datetime.timedelta(days=k)) + "'")
				elif ('int' in attrib_types_dict[(tabname_inner, attrib_inner)] or 'numeric' in attrib_types_dict[(tabname_inner, attrib_inner)]):
					#check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
					if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
						insert_values.append(min(filter_attrib_dict[(tabname_inner, attrib_inner)][0] + k, filter_attrib_dict[(tabname_inner, attrib_inner)][1]))
					else:
						insert_values.append(dummy_int + k)
				else:
					if (tabname_inner, attrib_inner) in filter_attrib_dict.keys():
						temp = copy.deepcopy(filter_attrib_dict[(tabname_inner, attrib_inner)])
						if '_' in filter_attrib_dict[(tabname_inner, attrib_inner)]:
							insert_values.append(temp.replace('_', chr(dummy_char + k)))
						else:
							insert_values.append(temp.replace('%', chr(dummy_char + k), 1))
						insert_values[-1].replace('%', '')
					else:
						insert_values.append(chr(dummy_char + k))
			flag = 1
			insert_rows.append(tuple(insert_values))
		esc_string = '(' + '%s'
		for k in range(1, len(attrib_list_inner)):
			esc_string = esc_string + ", " + '%s'
		esc_string = esc_string + ")"
		cur = reveal_globals.global_conn.cursor()
		att_order=att_order[:-1]
		att_order+=')'
		query="INSERT INTO " + tabname_inner + att_order + " VALUES "+ esc_string
		for x in insert_rows:
			cur.execute(query, x)
		#insert multiple rows together
		#cur.execute("INSERT INTO " + tabname_inner + " VALUES " + esc_string, insert_rows)
		# args_str = ','.join(cur.mogrify(esc_string, x).decode("utf-8") for x in insert_rows)
		# cur.execute("INSERT INTO " + tabname_inner + att_order + " VALUES " + args_str)
		cur.close()
		reveal_globals.global_min_instance_dict['limit_' + tabname_inner + '_D_gen1'] = copy.deepcopy(insert_rows)
		reveal_globals.global_min_instance_dict['limit_' + tabname_inner + '_D_gen1'].insert(0, copy.deepcopy(reveal_globals.global_min_instance_dict[tabname_inner][0]))
		reveal_globals.local_other_info_dict['Table ' + tabname_inner] = str(len(insert_rows)) + ' row(s)'
	new_result = executable.getExecOutput()
	reveal_globals.global_result_dict['limit_D_gen1'] = copy.deepcopy(new_result)
	reveal_globals.local_other_info_dict['Target Result Cardinality'] = str(no_of_rows) + ' row(s)'
	reveal_globals.local_other_info_dict['Actual Result Cardinality'] = str(len(new_result) - 1) + ' row(s)'
	return_val = None
	if len(new_result) <= 1:
		reveal_globals.error="Unmasque Error: \n some error in generating new database. Result is empty. Can not identify Limit."
		print('some error in generating new database. Result is empty. Can not identify Limit.')
		return None
	else:
		if len(new_result) <= 1000:
			reveal_globals.local_other_info_dict['Conclusion'] = 'Limit is present with value '+ str(len(new_result) - 1)
			return_val = len(new_result) - 1
		else:
			reveal_globals.local_other_info_dict['Conclusion'] = 'Limit, if prsent, has value greater than ' + str(len(new_result) - 1)
	reveal_globals.global_other_info_dict['limit_D_gen1'] = copy.deepcopy(reveal_globals.local_other_info_dict)
	return return_val