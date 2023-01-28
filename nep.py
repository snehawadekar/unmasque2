from hashlib import new
import os
import sys
import csv
import copy
import math
from turtle import radians, st
import executable
import datetime
from nep_minimizer import getStrFilterValue
sys.path.append('../') 
import reveal_globals
import psycopg2
import time
import where_clause 
import decimal
import random


def getCoreSizes(core_relations):
	core_sizes = {}
	for tabname in core_relations:
		try:
			cur = reveal_globals.global_conn.cursor()
			cur.execute('select count(*) from ' + tabname + '1;')
			res = cur.fetchone()
			cur.close()
			core_sizes[tabname] = int(str(res[0]))
		except Exception as error:
			print("Error in getting table Sizes. Error: " + str(error))
	return core_sizes


def match1(Q_E, new_result):
    cur  = reveal_globals.global_conn.cursor()
    cur.execute(Q_E)
    res = cur.fetchall()
    cur.close()
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
            return False   
    return True 

def match(Q_E,new_result):
    global tim 
    start_time = time.time()
    # Run the extracted query Q_E .
    cur  = reveal_globals.global_conn.cursor()
    cur.execute('create view temp1 as '+ Q_E)
    cur.close()

    # Size of the table
    cur  = reveal_globals.global_conn.cursor()
    cur.execute('select count(*) from temp1;')
    res = cur.fetchone()
    res = res[0]
    cur.close()

    if(res < 5000):
        # Drop the temporary table and view created.
        cur = reveal_globals.global_conn.cursor()
        cur.execute("drop view temp1;")
        cur.close()
        return match1(Q_E, new_result)

    # Create an empty table with name temp2
    cur = reveal_globals.global_conn.cursor()
    cur.execute('Create unlogged table temp2 (like temp1);')
    cur.close()

    # Header of temp2
    t = new_result[0]
    t1 = '(' + t[0]
    for i in range(1,len(t)):
        t1 += ', ' + t[i]
    t1 += ')'

    # Filling the table temp2
    for i in range(1,len(new_result)):
        cur = reveal_globals.global_conn.cursor()
        cur.execute('INSERT INTO temp2'+str(t1)+' VALUES'+str(new_result[i])+'; ')
        cur.close()

   
    cur  = reveal_globals.global_conn.cursor()
    cur.execute('select count(*) from (select * from temp1 except all select * from temp2) as T;')
    len1 = cur.fetchone()
    len1 = len1[0]
    cur.close()

    cur  = reveal_globals.global_conn.cursor()
    cur.execute('select count(*) from (select * from temp2 except all select * from temp1) as T;')
    len2 = cur.fetchone()
    len2 = len2[0]
    cur.close()

    # Drop the temporary table and view created.
    cur = reveal_globals.global_conn.cursor()
    cur.execute("drop view temp1;")
    cur.close()
    
    cur = reveal_globals.global_conn.cursor()
    cur.execute("drop table temp2;")
    cur.close()
    
    if(len1 == 0 and len2 == 0):
        return True
    else:
        return False


def extractNEPValue(tabname,i):

    #print the table content
    cur = reveal_globals.global_conn.cursor()
    cur.execute("SELECT * FROM " + tabname +";")
    res = cur.fetchall()
    print(res)
    cur.close()

    #Return if hidden executable is giving non-empty output on the reduced database
    # It means that the current table doesnot contain NEP source column
    new_result = executable.getExecOutput()
    reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
    if(len(new_result) > 1):
        return False

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

    
    attrib_list = reveal_globals.global_all_attribs[i]
    filterAttribs = []

    #convert the view into a table
    cur = reveal_globals.global_conn.cursor()
    cur.execute("alter view "+tabname +" rename to " + tabname +"_nep ;")
    cur.close()

    cur = reveal_globals.global_conn.cursor()
    cur.execute("create table "+tabname +" as select * from " + tabname +"_nep ;")
    cur.close()
 
    for attrib in attrib_list:
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
                print("UPDATE " + tabname + " SET " + attrib +" = " + str(val) +";")
            else:
                cur.execute("UPDATE " + tabname + " SET " + attrib +" = '" + val +"';")    
                print("UPDATE " + tabname + " SET " + attrib +" = '" + val +"';")    
            cur.close()

            new_result = executable.getExecOutput()
            reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
            # To make decimal precision 
            
            #If it is non-empty
            if(len(new_result) > 1):
                print(new_result,"yessssssss")
                if 'int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[(tabname, attrib)]:
                    prev = "{0:.2f}".format(prev)
                    prev = decimal.Decimal(prev)
                print("yes",prev)
                reveal_globals.local_other_info_dict['Conclusion'] = 'Filter predicate on ' + attrib +' with operator <>'
                reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict)
                filterAttribs.append((tabname, attrib, '<>', prev))
                reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + attrib +' <> '+ str(prev)
                reveal_globals.global_other_info_dict['filter_'+attrib+'_D_mut'+str(reveal_globals.local_instance_no - 1)] = copy.deepcopy(reveal_globals.local_other_info_dict) 
                print(filterAttribs,'++++++_______++++++')
                
            
                #convert the table back to view
                cur = reveal_globals.global_conn.cursor()
                cur.execute("drop table "+tabname +";")
                cur.close()

                cur = reveal_globals.global_conn.cursor()
                cur.execute("alter view "+tabname +"_nep rename to " + tabname +";")
                cur.close()
              
                return filterAttribs 

    #convert the table back to view
 
    cur = reveal_globals.global_conn.cursor()
    cur.execute("drop table "+tabname +";")
    cur.close()

    cur = reveal_globals.global_conn.cursor()
    cur.execute("alter view "+tabname +"_nep rename to " + tabname +";")
    cur.close()  
                       
    return False


def updatedExtractedQuery(tabname,Q_E,i):
    val = extractNEPValue(tabname,i)
    if(val != False):
        for elt in val:
            predicate = ''
            if '-' in str(elt[3]):
                predicate = elt[1] + " " + str(elt[2]) + " '" + str(elt[3]) + "' "
                reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + elt[1] +' <> '+str(elt[3])
            elif isinstance(elt[3],str):
                print("+++++",elt[0],elt[1],elt[3],reveal_globals.global_attrib_max_length[elt[0],elt[1]])
                output = getStrFilterValue(elt[0],elt[1],elt[3],reveal_globals.global_attrib_max_length[elt[0],elt[1]])
                print(output,"++___++")
                if('%' in output or '_' in output):
                    predicate = elt[1] + " NOT LIKE '" + str(output) + "' "
                    reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + elt[1] +' NOT LIKE '+str(elt[3])
                else:
                    predicate = elt[1] + " " + str(elt[2]) + " '" + str(output) + "' "
                    reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + elt[1] +' <> '+str(elt[3])
            else:
                predicate = elt[1] + " " + str(elt[2]) + " " + str(elt[3])
                reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + elt[1] +' <> '+str(elt[3])

            if reveal_globals.global_where_op == '':
                reveal_globals.global_where_op = predicate
            else:
                reveal_globals.global_where_op = reveal_globals.global_where_op + " and " + predicate
		
        Q_E = "Select " + reveal_globals.global_select_op_proc + "\n" + "From "  + reveal_globals.global_from_op
        if reveal_globals.global_where_op.strip() != '':
            Q_E = Q_E + "\n" + "Where " + reveal_globals.global_where_op
        if reveal_globals.global_groupby_op.strip() != '':
            Q_E = Q_E + "\n" + "Group By " + reveal_globals.global_groupby_op
        if reveal_globals.global_orderby_op.strip() != '':
            Q_E = Q_E + "\n" + "Order By " + reveal_globals.global_orderby_op
        if reveal_globals.global_limit_op.strip() != '':
            Q_E = Q_E + "\n" + "Limit " + reveal_globals.global_limit_op 
        Q_E = Q_E + ";"
        print("+++++",Q_E)
        return Q_E
    else:
        return Q_E

def nep_db_minimizer(tabname,Q_E,core_sizes,partition_dict,i): 
    print("HELLO")
    #Run the hidden query on this updated database instance with table T_u
    new_result = executable.getExecOutput()
    reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1

    #Base Case
    if(core_sizes == 1 and match(Q_E,new_result) == False):
        return updatedExtractedQuery(tabname,Q_E,i)   
    
    # Drop the current view of name tabname
    cur = reveal_globals.global_conn.cursor()
    cur.execute("drop view "+tabname+ ";")
    cur.close()


    # Make a view of name x with first half  T <- T_u
    cur = reveal_globals.global_conn.cursor()
    cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1 order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[0])) + ' limit ' + str(int(partition_dict[1]/2)) + ';')
    cur.close()


    if(match(Q_E,new_result) == False):
        Q_E_ = nep_db_minimizer(tabname,Q_E, int(partition_dict[1]/2), (int(partition_dict[0]), int(partition_dict[1]/2)),i)

        # Drop the view of name tabname
        cur = reveal_globals.global_conn.cursor()
        cur.execute("drop view "+tabname+ ";")
        cur.close()
        
        # Make a view of name x with second half  T <- T_l
        cur = reveal_globals.global_conn.cursor()
        cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1 order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[0]) + int(partition_dict[1]/2)) + ' limit ' + str(int(partition_dict[1]) - int(partition_dict[1]/2)) + ';')
        cur.close()
       
        #Run the hidden query on this updated database instance with table T_l
        new_result = executable.getExecOutput()
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1

        if(match(Q_E_,new_result) == False):
            Q_E__ = nep_db_minimizer(tabname,Q_E_, int(partition_dict[1]) - int(partition_dict[1]/2), (int(partition_dict[0]) + int(partition_dict[1]/2), int(partition_dict[1]) - int(partition_dict[1]/2)), i)
            return Q_E__
        else:
            return Q_E_
    else:
        # Drop the view of name tabname
        cur = reveal_globals.global_conn.cursor()
        cur.execute("drop view "+tabname+ ";")
        cur.close()

        #Make a view of name x with second half  T <- T_l
        cur = reveal_globals.global_conn.cursor()
        cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1 order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[0]) + int(partition_dict[1]/2)) + ' limit ' + str(int(partition_dict[1]) - int(partition_dict[1]/2)) + ';')
        cur.close()

        #Run the hidden query on this updated database instance with table T_u
        new_result = executable.getExecOutput()
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
    

        if(match(Q_E,new_result) == False):
            Q_E_ = nep_db_minimizer(tabname,Q_E, int(partition_dict[1]) - int(partition_dict[1]/2), ( int(partition_dict[0]) + int(partition_dict[1]/2), int(partition_dict[1]) - int(partition_dict[1]/2)),i)
            return Q_E_
        else:
            return Q_E


def nep_db_minimizer1(tabname,Q_E,core_sizes,partition_dict,i): 
    #Run the hidden query on this updated database instance with table T_u
    print("HELLO",core_sizes)
    new_result = executable.getExecOutput()
    reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1

    #Base Case
    if(core_sizes == 1 and match(Q_E,new_result) == False):
        print("YES FOUND")
        return updatedExtractedQuery(tabname,Q_E,i)   
    
    # Drop the current table of name tabname
    cur = reveal_globals.global_conn.cursor()
    cur.execute("drop table "+tabname+ ";")
    cur.close()


    # Make a table of name x with first half  T <- T_u
    cur = reveal_globals.global_conn.cursor()
    cur.execute('create table ' + tabname + ' as select * from '+ tabname +'1 order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[0])) + ' limit ' + str(int(partition_dict[1]/2)) + ';')
    cur.close()


    if(match(Q_E,new_result) == False):
        Q_E_ = nep_db_minimizer(tabname,Q_E, int(partition_dict[1]/2), (int(partition_dict[0]), int(partition_dict[1]/2)),i)

        # Drop the table of name tabname
        cur = reveal_globals.global_conn.cursor()
        cur.execute("drop table "+tabname+ ";")
        cur.close()
        
        # Make a table of name x with second half  T <- T_l
        cur = reveal_globals.global_conn.cursor()
        cur.execute('create table ' + tabname + ' as select * from '+ tabname +'1 order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[0]) + int(partition_dict[1]/2)) + ' limit ' + str(int(partition_dict[1]) - int(partition_dict[1]/2)) + ';')
        cur.close()
       
        #Run the hidden query on this updated database instance with table T_l
        new_result = executable.getExecOutput()
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1

        if(match(Q_E_,new_result) == False):
            Q_E__ = nep_db_minimizer(tabname,Q_E_, int(partition_dict[1]) - int(partition_dict[1]/2), (int(partition_dict[0]) + int(partition_dict[1]/2), int(partition_dict[1]) - int(partition_dict[1]/2)), i)
            return Q_E__
        else:
            return Q_E_
    else:
        # Drop the table of name tabname
        cur = reveal_globals.global_conn.cursor()
        cur.execute("drop table "+tabname+ ";")
        cur.close()

        #Make a table of name x with second half  T <- T_l
        cur = reveal_globals.global_conn.cursor()
        cur.execute('create table ' + tabname + ' as select * from '+ tabname +'1 order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[0]) + int(partition_dict[1]/2)) + ' limit ' + str(int(partition_dict[1]) - int(partition_dict[1]/2)) + ';')
        cur.close()

        #Run the hidden query on this updated database instance with table T_u
        new_result = executable.getExecOutput()
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
    

        if(match(Q_E,new_result) == False):
            Q_E_ = nep_db_minimizer(tabname,Q_E, int(partition_dict[1]) - int(partition_dict[1]/2), ( int(partition_dict[0]) + int(partition_dict[1]/2), int(partition_dict[1]) - int(partition_dict[1]/2)),i)
            return Q_E_
        else:
            return Q_E

def nep_algorithm(core_relations, Q_E):
    # get the size of all the tables
    core_sizes = getCoreSizes(core_relations)

    # STORE STARTING POINT(OFFSET) AND NOOFROWS(LIMIT) FOR EACH TABLE IN FORMAT (offset, limit)
    partition_dict = {}
    for key in core_sizes.keys():
        partition_dict[key] = (0, core_sizes[key])

    # Create the view of all the tables    
    for tabname in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('drop table ' + tabname + ';')
        cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1;')
        cur.close()

    # Run the hidden query on the original database instance
    new_result = executable.getExecOutput()
    reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
    print(reveal_globals.global_core_relations)
    if(match(Q_E,new_result) == False):
        # NEP may exists
        print("NEP may exists")
        for i in range(len(reveal_globals.global_core_relations)):
            tabname = reveal_globals.global_core_relations[i]
            Q_E_ = nep_db_minimizer(tabname,Q_E, core_sizes[tabname], partition_dict[tabname],i)
            Q_E = Q_E_
            #Run the hidden query on the original database instance
            new_result = executable.getExecOutput()
            reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
            
            if(match(Q_E,new_result) == True):
                break
        #drop all the views 
      
        for tabname in reveal_globals.global_core_relations:
            cur = reveal_globals.global_conn.cursor()
            cur.execute('alter view ' + tabname + ' rename to '+tabname+'3;')
            cur.execute('create table ' + tabname + ' as select * from '+tabname+'3;')
            cur.execute('drop view ' + tabname + '3;')
            cur.close()
          
        #print("Comparison time:",tim)
        #time.sleep(100)
        return Q_E    
            
    else:
        print("NEP doesn't exists under our assumptions")
       
        #drop all the views 
        for tabname in reveal_globals.global_core_relations:
            cur = reveal_globals.global_conn.cursor()
            cur.execute('alter view ' + tabname + ' rename to '+tabname+'3;')
            cur.execute('create table ' + tabname + ' as select * from '+tabname+'3;')
            cur.execute('drop view ' + tabname + '3;')
            cur.close()
          
        #print("Comparison time:",tim)
        return False

def getStrFilterValue(tabname, attrib, representative, max_length):
    
    #convert the view into a table
    cur = reveal_globals.global_conn.cursor()
    cur.execute("alter view "+tabname +" rename to " + tabname +"_nep ;")
    cur.close()

    cur = reveal_globals.global_conn.cursor()
    cur.execute("create table "+tabname +" as select * from " + tabname +"_nep ;")
    cur.close()    
    
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
            #update_other_data(tabname, attrib, 'text', temp, new_result, [])
            if len(new_result) <= 1:
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
      
        # convert the table back to view
        cur = reveal_globals.global_conn.cursor()
        cur.execute("drop table "+tabname +";")
        cur.close()

        cur = reveal_globals.global_conn.cursor()
        cur.execute("alter view "+tabname +"_nep rename to " + tabname +";")
        cur.close()
       
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
            #update_other_data(tabname, attrib, 'text', temp, new_result, [])
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
        #update_other_data(tabname, attrib, 'text', temp, new_result, [])
        if len(new_result) <= 1:
            output = output + '%'
    
    #convert the table back to view
    cur = reveal_globals.global_conn.cursor()
    cur.execute("drop table "+tabname +";")
    cur.close()

    cur = reveal_globals.global_conn.cursor()
    cur.execute("alter view "+tabname +"_nep rename to " + tabname +";")
    cur.close()
    
    return output

    