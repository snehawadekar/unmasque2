import os
import sys
import csv
import copy
import math
import executable
import datetime
sys.path.append('../') 
import reveal_globals
import psycopg2
import time
import where_clause 
import decimal
import random

#mukul
def getCoreSizes(core_relations):
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

def compare(res, new_result):
    new_result_ = new_result
    for i in range(len(res)):
        flag = 0
        temp = ()
        for ele in  (res[i]):
            ele = str(ele)
            temp += (ele,)
        for j in range(len(new_result)):
            if(temp == new_result_[j]):
                new_result_[j] = []
                flag = 1
                break
        if(flag == 0):
            print(res[i],"NOT found")
            return False   
    return True        

def extractNEPValue():
    # check nep for every non-key attribute by changing its value to different s value and run the executable.
    # If the output came out non- empty. It means that nep is present on that attribute with previous value.
    attrib_types_dict = {}
    for entry in reveal_globals.global_attrib_types:
        attrib_types_dict[(entry[0], entry[1])] = entry[2]

    dummy_int = 2   
    dummy_char = 65 # to avoid having space/tab
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
        attrib_list = reveal_globals.global_all_attribs[i]
        print(tabname)
        filterAttribs = []
        for attrib in attrib_list:
            print(attrib)
            if attrib not in reveal_globals.global_key_attributes:
                if 'date' in attrib_types_dict[(tabname, attrib)]:
                    if (tabname, attrib) in filter_attrib_dict.keys():
                        val = ("'" + str(min(filter_attrib_dict[(tabname, attrib)][0], filter_attrib_dict[(tabname, attrib)][1])) + "'")
                    else:
                        val = ("'" + str(dummy_date) + "'")

                elif('int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[(tabname, attrib)]):
                    #check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
                    if (tabname, attrib) in filter_attrib_dict.keys():
                        val = (min(filter_attrib_dict[(tabname, attrib)][0], filter_attrib_dict[(tabname, attrib)][1]))
                    else:
                        val = (dummy_int) 
                else:
                    if (tabname, attrib) in filter_attrib_dict.keys():
                        val = (filter_attrib_dict[(tabname, attrib)].replace('%', ''))
                    else:
                        val = (chr(dummy_char))
                print(val)
                cur = reveal_globals.global_conn.cursor()
                cur.execute("SELECT "+attrib +" FROM " + tabname +";")
                prev = cur.fetchone()
                print("Type of this prev",prev,type(prev))
                prev = prev[0]
                #prev = (''.join(map(str, prev)))
                cur.close()
                cur = reveal_globals.global_conn.cursor()
                if 'date' in attrib_types_dict[(tabname, attrib)]:
                    cur.execute("UPDATE " + tabname + " SET " + attrib +" = " + val +";")
                    print("UPDATE " + tabname + " SET " + attrib +" = " + val +";")
                elif('int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[(tabname, attrib)]):
                    cur.execute("UPDATE " + tabname + " SET " + attrib +" = " + str(val) +";")
                else:
                    cur.execute("UPDATE " + tabname + " SET " + attrib +" = '" + val +"';")        
                cur.close()
                new_result = executable.getExecOutput()
                reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
                # To make decimal precision 
                

                print(new_result,"yessssssss")
                if(len(new_result) > 1):
                    if 'int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[(tabname, attrib)]:
                        prev = "{0:.2f}".format(prev)
                        prev = decimal.Decimal(prev)
                    print("yes",prev)
                    reveal_globals.local_other_info_dict['Conclusion'] = 'Filter predicate on ' + attrib +' with operator <>'
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                    filterAttribs.append((tabname, attrib, '<>', prev))
                    reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' <> '+ str(prev)
                    reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict) 
                    return filterAttribs
    
    return False

def reduce_Database_Instance(core_relations, extractedQuery,method = 'binary partition', max_no_of_rows = 1, executable_path = ""):
    
    '''
    # create new tables with the name tabname1 
    for tabname in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("create unlogged table " + tabname + "1" + " (like " + tabname + ");")
        cur.execute("Insert into " + tabname + "1" + " select * from " + tabname + ";")
        cur.execute('alter table ' + tabname + "1" +' add primary key(' + reveal_globals.global_pk_dict[tabname] + ');')
        cur.close()
    '''
    # Run the extracted query on the original database instance 
    cur  = reveal_globals.global_conn.cursor()
    cur.execute(extractedQuery)
    res = cur.fetchall()

    #Run the hidden query on the original database instance
    new_result = executable.getExecOutput()
    reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
    #new_result.pop(0)
    print("++++++++++==============++++++++++++")
    print(new_result[0],new_result[1])

    cur  = reveal_globals.global_conn.cursor()
    cur.execute('INSERT INTO p2'+ t1 + ' VALUES' + str(new_result[1])+'; ')
    cur.close()
    print(len(res), "extractedQuery")
    print(len(new_result), "hidden query ")
    #Check whether there size is same or not
    if(len(res) == len(new_result)):
        if(compare(res,new_result) == True):
            print("False and return ")
            return False
        #nep may exists
        core_sizes = getCoreSizes(core_relations)
        #STORE STARTING POINT(OFFSET) AND NOOFROWS(LIMIT) FOR EACH TABLE IN FORMAT (offset, limit)
        partition_dict = {}
        for key in core_sizes.keys():
            partition_dict[key] = (0, core_sizes[key])
        #indexnumber as index has to be unique each time
        index_no = 70
        key_max = max(core_sizes.keys(), key=(lambda k: core_sizes[k]))
        tabname = key_max
        while(1):
            print(tabname+" "+str(core_sizes[tabname]))
            # Rename Current table x to temp
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Alter table ' + tabname + ' rename to temp;')
            cur.close()

            # Create an empty table with name x
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

            #Run the extracted query and hidden query on the half table
            new_result = executable.getExecOutput()
            reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
            new_result.pop(0)
            cur  = reveal_globals.global_conn.cursor()
            cur.execute(extractedQuery)
            res = cur.fetchall()

            print("Extracted",res)
            print("Hidden",new_result)
            if((res == [(None,)] or res == []) and new_result == []):
                # CASE 1
                cur = reveal_globals.global_conn.cursor()
                cur.execute('drop table ' + tabname + ';')
                cur.close()
                cur = reveal_globals.global_conn.cursor()
                cur.execute("Alter table temp rename to " + tabname + ";")
                cur.close()
                partition_dict[tabname] = ( partition_dict[tabname][0] + int(partition_dict[tabname][1]/2), int(partition_dict[tabname][1]) - int(partition_dict[tabname][1]/2) )
            if(res != [(None,)] and res != [] and new_result == []):    
                # CASE 2
                cur = reveal_globals.global_conn.cursor()
                cur.execute('drop table temp;')
                partition_dict[tabname] = (0, int(partition_dict[tabname][1]/2))
                cur.close()
            if(len(res) >= 1 and len(new_result) >= 1):
                if(compare(res,new_result) == False):
                    # CASE 3
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('drop table temp;')
                    partition_dict[tabname] = (0, int(partition_dict[tabname][1]/2))
                    cur.close() 
                else:
                    # CASE 4
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute('drop table ' + tabname + ';')
                    cur.close()
                    cur = reveal_globals.global_conn.cursor()
                    cur.execute("Alter table temp rename to " + tabname + ";")
                    cur.close()
                    partition_dict[tabname] = ( partition_dict[tabname][0] + int(partition_dict[tabname][1]/2), int(partition_dict[tabname][1]) - int(partition_dict[tabname][1]/2) )      
            core_sizes[tabname] = int(partition_dict[tabname][1])

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
                if(core_sizes[tabname] == 1):
                    del core_sizes[tabname]
                    if(not bool(core_sizes)):
                        break
                    key_max = max(core_sizes.keys(), key=(lambda k: core_sizes[k]))
                    tabname = key_max
                
        return extractNEPValue()


    elif(len(res) > len(new_result)):
        #nep may exists
        core_sizes = getCoreSizes(core_relations)
        #STORE STARTING POINT(OFFSET) AND NOOFROWS(LIMIT) FOR EACH TABLE IN FORMAT (offset, limit)
        partition_dict = {}
        for key in core_sizes.keys():
            partition_dict[key] = (0, core_sizes[key])
        #indexnumber as index has to be unique each time
        index_no = 90
        while(1):
            if(not bool(core_sizes)):
                break
            key_max = max(core_sizes.keys(), key=(lambda k: core_sizes[k]))
            tabname = key_max

            # Rename Current table x to temp
            cur = reveal_globals.global_conn.cursor()
            cur.execute('Alter table ' + tabname + ' rename to temp;')
            cur.close()

            # Create an empty table with name x
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

            #Run the extracted query and hidden query on the half table
            new_result = executable.getExecOutput()
            reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
            new_result.pop(0)
            cur  = reveal_globals.global_conn.cursor()
            cur.execute(extractedQuery)
            res = cur.fetchall()
            if(len(res) > len(new_result)):
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
        return extractNEPValue()
        
    else:
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
		if len(new_result) <= 1:
			reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[index] + "' is a replacement for wildcard character '%' or '_' in NOT LIKE"
			reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
			temp = copy.deepcopy(representative)
			temp = temp[:index] + temp[index+1:]
			#updatequery
			query = "update " + tabname + " set " + attrib + " = " + "'" + temp + "';"
			cur = reveal_globals.global_conn.cursor()
			cur.execute(query)
			cur.close()
			new_result = executable.getExecOutput()
			if len(new_result) <= 1:
				reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[index] + "' is a replacement from wildcard character '%' in NOT LIKE"
				reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
				representative = representative[:index] + representative[index+1:]
			else:
				reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[index] + "' is a replacement from wildcard character '_' in NOT LIKE"
				reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
				output = output + "_"
				representative = list(representative)
				representative[index] = u"\u00A1"
				representative = ''.join(representative)
				index = index + 1
		else:
			reveal_globals.local_other_info_dict['Conclusion'] = "'" + representative[index] + "' is an intrinsic character in filter value for NOT LIKE"
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
			if len(new_result) <= 1:
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
		if len(new_result) <= 1:
			output = output + '%'
	return output


    