from hashlib import new
import sys
import copy
from turtle import radians, st
import executable
import datetime
from nep_minimizer import getStrFilterValue
sys.path.append('../') 
import reveal_globals
import time
import decimal
import check_nullfree
import outer_join


def getCoreSizes(core_relations):
    # chnage this for 100GB 
	core_sizes = {}
	for tabname in core_relations:
		try:
			cur = reveal_globals.global_conn.cursor()
			cur.execute('select count(*) from ' + tabname + '_restore;')
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
    # print('create view temp1 as '+ Q_E)
    cur.execute('create view temp1 as '+ Q_E)
    cur.close()

    # Size of the table
    x = time.time()
    cur  = reveal_globals.global_conn.cursor()
    cur.execute('select count(*) from temp1;')
    res = cur.fetchone()
    res = res[0]
    cur.close()
    # print(res)
    # print(time.time() - x)

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
        flag_skip = False
        # for k in new_result[i]:
        #     if k=='None':
        #         flag_skip = True
        if flag_skip == False:
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

def match_oj_temp(Q_E,new_result):
    
    start_time = time.time()
    
    # Run the extracted query Q_E.
    cur  = reveal_globals.global_conn.cursor()
    # print('create view temp1 as '+ Q_E)
    cur.execute(' create table temp1 as ' + Q_E)
    cur.execute(' truncate table temp1 ')
    cur.execute(Q_E)
    res_oj = cur.fetchall()
    colnames = [desc[0] for desc in cur.description] 
    
    cur.close()
    result = []
    result.append(colnames)
    datatype_col = []
    for i in range(len(cur.description)):
        # print("Column {}:".format(i+1))
        desc = cur.description[i]
        datatype_col.append( desc[1] )
       
            
    for row in res_oj:
        #CHECK IF THE WHOLE ROW IN NONE (SPJA Case)
        temp = []
        for val in row:
            # if val == 'None':
            #     temp.append(str('Null'))
            # else:
            temp.append(str(val))
        result.append((temp))
    
    # t = colnames
    # t1 = '(' + t[0]
    # for i in range(1,len(t)):
    #     t1 += ', ' + t[i]
    # t1 += ')'
    
    
    for i in range(1,len(result)):
        insert_values=[]
        new_t=[]
        t = colnames
        flag_skip = False
        for k in range(len(result[i])):
            if result[i][k]=='None' :
                pass
                # pc=t[k]
                # t.pop(t.index(pc))
            else:
                new_t.append(t[k])
                insert_values.append( result[i][k] )
                # if datatype_col[k] == 23 or datatype_col[k] == 1700 :
                #     result[i][k] = 0
                # elif datatype_col[k] == 1042:
                #     result[i][k] = 'N'
                # elif datatype_col[k] == 20 : # bigint
                #     result[i][k] = 0
                # elif datatype_col[k] == 1082 : #date,time
                #     result[i][k] = '1999-01-01'
                # else:
                #     s = "NULL"
                #     result[i][k] = s
                # flag_skip = True
        
        t1 = '(' + new_t[0]
        for i in range(1,len(new_t)):
            t1 += ', ' + new_t[i]
        t1 += ')'
        
        if flag_skip == False:
            cur = reveal_globals.global_conn.cursor()
            # print( 'INSERT INTO temp1'+str(t1)+' VALUES'+str(tuple(result[i]))+'; ' )
            cur.execute('INSERT INTO temp1'+str(t1)+' VALUES'+str(tuple(insert_values))+'; ')
            # print( 'INSERT INTO temp1'+str(t1)+' VALUES'+str(tuple(result[i]))+'; ' )
            cur.close()

   
   
    # Create an empty table with name temp2
    cur = reveal_globals.global_conn.cursor()
    cur.execute('Create table temp2 (like temp1);')
    cur.close()


    result2 = []
    for row in new_result:
        #CHECK IF THE WHOLE ROW IN NONE (SPJA Case)
        temp = []
        for val in row:
            # if val == 'None':
            #     temp.append(str('Null'))
            # else:
            temp.append(str(val))
        result2.append((temp))
    
    # Header of temp2
    t = result2[0]
    t1 = '(' + t[0]
    for i in range(1,len(t)):
        t1 += ', ' + t[i]
    t1 += ')'

    # Filling the table temp2
    for i in range(1,len(result2)):
        insert_values=[]
        new_t=[]
        flag_skip = False
        for k in range(len(result2[i])):
            if result2[i][k]=='None' :
                pass
                # pc=t[k]
                # t.pop(t.index(pc))
            else:
                new_t.append(t[k])
                insert_values.append( result2[i][k] )
                # if datatype_col[k] == 23 or datatype_col[k] == 1700 :
                #     result2[i][k] = 0
                # elif datatype_col[k] == 1042:
                #     result2[i][k] = 'N'
                # elif datatype_col[k] == 20 : # bigint
                #     result2[i][k] = 0
                # elif datatype_col[k] == 1082 : #date,time
                #     result2[i][k] = '1999-01-01'
                # else:
                #     s = "NULL"
                #     result2[i][k] = s
                
        t1 = '(' + new_t[0]
        for i in range(1,len(new_t)):
            t1 += ', ' + new_t[i]
        t1 += ')'
        
        if flag_skip == False:
            cur = reveal_globals.global_conn.cursor()
            cur.execute('INSERT INTO temp2' + str(t1) + ' VALUES'+str(tuple(insert_values)) + '; ')
            cur.close()

   
   
    ## temp1 = extracted query
    ## temp2 = hidden query
    ## for nep row must be present in EQ byt not in HQ
    cur  = reveal_globals.global_conn.cursor()
    cur.execute('select count(*) from (select * from temp1 except select * from temp2) as T;')
    len1 = cur.fetchone()
    len1 = len1[0]
    cur.close()
    
    # cur  = reveal_globals.global_conn.cursor()
    # cur.execute('select * from (select * from temp1 except all select * from temp2) as T;')
    # r1 = cur.fetchall()
    # print(r1)
    # cur.close()

    # commented 14-05-2023
    # cur  = reveal_globals.global_conn.cursor()
    # cur.execute('select count(*) from (select * from temp2 except all select * from temp1) as T;')
    # len2 = cur.fetchone()
    # len2 = len2[0]
    # cur.close()

    # cur  = reveal_globals.global_conn.cursor()
    # cur.execute('select * from (select * from temp2 except all select * from temp1) as T;')
    # r2 = cur.fetchall()
    # print(r2)
    # cur.close()


    # Drop the temporary table and view created.
    cur = reveal_globals.global_conn.cursor()
    cur.execute("drop table temp1;")
    cur.close()
    
    cur = reveal_globals.global_conn.cursor()
    cur.execute("drop table temp2;")
    cur.close()
    
    # if(len1 == 0 and len2 == 0):
    if len1 == 0:
        return True
    else:
        return False

def match_oj(Q_E):
    
    try:
        cur = reveal_globals.global_conn.cursor()
        hidden_query=reveal_globals.query1
        # extracted_query = reveal_globals.output1
        extracted_query = Q_E
        # print("query=",query)
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
        cur.execute(hidden_query)
        res_hq = cur.fetchall() #fetchone always return a tuple whereas fetchall return list
        cur.close()
        print(len(res_hq))
        
        cur = reveal_globals.global_conn.cursor()
        
        cur.execute(extracted_query)
        res_eq = cur.fetchall() # fetchone always return a tuple whereas fetchall return list
        cur.close()
        print(len(res_eq))
        
        # temp3=[] # outer join
        # for element in res_hq:
        #     if element not in res_eq:
        #         temp3.append(element)
        #         # reveal_globals.outer_join_flag = True
        #         return False
                
        # if len(res_eq) == len(res_hq):
        #     return True
                
                
        temp4=[] # nep 
        for element in res_eq:
            if element not in res_hq:
                temp4.append(element)
                reveal_globals.nep_flag = True
                return False
                    
        # cur = reveal_globals.global_conn.cursor()
        # cur.execute("SELECT * FROM ("+ hidden_query + "\n  EXCEPT " + extracted_query +") as foo;")
        # res_1 = cur.fetchall() #fetchone always return a tuple whereas fetchall return list
        # cur.close()
        # print(len(res_1))
        
        # cur = reveal_globals.global_conn.cursor()
        # cur.execute("SELECT * FROM ("+ extracted_query + ";\n  EXCEPT " + hidden_query +") as foo;")
        # res_1 = cur.fetchall() #fetchone always return a tuple whereas fetchall return list
        # cur.close()
        # print(len(res_1))
   
    except Exception as error:
        # reveal_globals.error='Unmasque Error: \n Executable could not be run. Error: ' +  dict(error.args[0])['M']
        print('Executable could not be run. Error: ' + str(error))
        raise error
    return True

def extractNEPValue(tabname,i):

    #print the table content
    cur = reveal_globals.global_conn.cursor()
    cur.execute("SELECT * FROM " + tabname +";")
    res = cur.fetchall()
    # print(res)
    cur.close()

    #Return if hidden executable is giving non-empty output on the reduced database
    # It means that the current table doesnot contain NEP source column
    # new_result = executable.getExecOutput()
    # reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
    # if(len(new_result) > 1):
    #     return False
    
    new_result = check_nullfree.getExecOutput()
    if new_result:
        # print("not extractable query , check with database")
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
    
    temp = []
    for i in attrib_list:
        if i not in reveal_globals.algebraic_pred_attributes:
            temp.append(i)
            
    attrib_list= temp
    
  
    for attrib in (attrib_list) :
        print(tabname,attrib)
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
            # print(val)
           
            cur = reveal_globals.global_conn.cursor()
            cur.execute("SELECT "+attrib +" FROM " + tabname +";")
            prev = cur.fetchone()
            # print("Type of this prev",prev,type(prev))
            prev = prev[0]
            #prev = (''.join(map(str, prev)))
            cur.close()
            cur = reveal_globals.global_conn.cursor()
            if 'date' in attrib_types_dict[(tabname, attrib)]:
                cur.execute("UPDATE " + tabname + " SET " + attrib +" = " + val +";")
                # print("UPDATE " + tabname + " SET " + attrib +" = " + val +";")
            elif('int' in attrib_types_dict[(tabname, attrib)] or 'numeric' in attrib_types_dict[(tabname, attrib)]):
                cur.execute("UPDATE " + tabname + " SET " + attrib +" = " + str(val) +";")
                # print("UPDATE " + tabname + " SET " + attrib +" = " + str(val) +";")
            else:
                cur.execute("UPDATE " + tabname + " SET " + attrib +" = '" + val +"';")    
                # print("UPDATE " + tabname + " SET " + attrib +" = '" + val +"';")    
            cur.close()

            # new_result = executable.getExecOutput()
            reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
            # To make decimal precision 
            
            # If it is non-empty
            new_result = check_nullfree.getExecOutput()

            # if(len(new_result) > 1):
            if new_result:
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
    if(val != False and (val[0][1] not in reveal_globals.algebraic_pred_attributes) and val not in reveal_globals.global_filter_predicates_disj):
        reveal_globals.check_nep_again = True
        # add the new predicate to the existing filter predicates
        reveal_globals.global_filter_predicates_disj.append(val)
        # for elt in val:
        #     predicate = ''
        #     if '-' in str(elt[3]):
        #         predicate = elt[1] + " " + str(elt[2]) + " '" + str(elt[3]) + "' "
        #         reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + elt[1] +' <> '+str(elt[3])
        #     elif isinstance(elt[3],str):
        #         print("+++++",elt[0],elt[1],elt[3],reveal_globals.global_attrib_max_length[elt[0],elt[1]])
        #         output = getStrFilterValue(elt[0],elt[1],elt[3],reveal_globals.global_attrib_max_length[elt[0],elt[1]])
        #         print(output,"++___++")
        #         if('%' in output or '_' in output):
        #             predicate = elt[1] + " NOT LIKE '" + str(output) + "' "
        #             reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + elt[1] +' NOT LIKE '+str(elt[3])
        #         else:
        #             predicate = elt[1] + " " + str(elt[2]) + " '" + str(output) + "' "
        #             reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + elt[1] +' <> '+str(elt[3])
        #     else:
        #         predicate = elt[1] + " " + str(elt[2]) + " " + str(elt[3])
        #         reveal_globals.local_other_info_dict['Conclusion'] = u'Filter Predicate is \u2013 ' + elt[1] +' <> '+str(elt[3])

        #     # reveal_globals.global_where_op = dict["where"]
        #     if reveal_globals.global_where_op == '':
        #         reveal_globals.global_where_op = predicate
        #     else:
        #         reveal_globals.global_where_op = reveal_globals.global_where_op + " and " + predicate

        
        # again run formulate queries funtion to get new queries
        
        
        for tab in reveal_globals.global_core_relations:
            cur = reveal_globals.global_conn.cursor()
            cur.execute('drop view if exists ' + tab + ';')
            cur.close()
            
            cur = reveal_globals.global_conn.cursor()
            cur.execute('create table ' + tab + ' as select * from '  + tab + '4;')   
            cur.close()
            
        
        reveal_globals.sem_eq_queries, reveal_globals.sem_eq_listdict = outer_join.FormulateQueries(reveal_globals.final_edge_seq)
        
        for tab in reveal_globals.global_core_relations:
            
            cur = reveal_globals.global_conn.cursor()
            cur.execute('drop table if exists ' + tab + ';')
            cur.close()
        
            if tab == tabname:
                cur = reveal_globals.global_conn.cursor()
                cur.execute('create view ' + tab + ' as select * from '  + tab + '4;')   
                cur.close()
            else:
                cur = reveal_globals.global_conn.cursor()
                cur.execute('create view ' + tab + ' as select * from '  + tab + '_restore;')   
                cur.close()
                
        
        return reveal_globals.sem_eq_queries[0]
    else:
        
        return Q_E

def nep_db_minimizer(tabname,Q_E,core_sizes,partition_dict,i ): 
    print("HELLO")
    #Run the hidden query on this updated database instance with table T_u
    # new_result = executable.getExecOutput()
    # reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1

    #Base Case
    # if(core_sizes == 1 and match_oj(Q_E,new_result) == False):
    if(core_sizes == 1 and match_oj(Q_E) == False):
        # for tab in reveal_globals.global_core_relations:
        #     # if tab != tabname:
        #     cur = reveal_globals.global_conn.cursor()
        #     cur.execute('Select count(*) from ' + tab+ ';')
        #     # cur.execute('truncate view '+ tab +';')
        #     # cur.execute('Insert into ' + tab + ' select * from ' + tab + '4;')
        #     res = cur.fetchall()
        #     print(res)
        #     cur.close()
            

        return updatedExtractedQuery(tabname,Q_E,i  )   
    
    # Drop the current view of name tabname
    cur = reveal_globals.global_conn.cursor()
    cur.execute("drop view "+tabname+ ";")
    cur.close()

    # Make a view of name x with first half  T <- T_u
    cur = reveal_globals.global_conn.cursor()
    cur.execute('create view ' + tabname + ' as select * from ' + tabname + '_restore order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[0])) + ' limit ' + str(int(partition_dict[1]/2)) + ';')
    # cur.execute('select count(*) from '+tabname +';')
    # res = cur.fetchone()
    # print(res)
    cur.close()

    # if( match_oj (Q_E, new_result) == False ):
    if( match_oj (Q_E) == False ):
        Q_E_ = nep_db_minimizer(tabname, Q_E, int(partition_dict[1]/2), (int(partition_dict[0]), int(partition_dict[1]/2)),i)
        print(Q_E_)
        
        if Q_E_ == Q_E or Q_E_ == True:
            return True


        # Drop the view of name tabname
        cur = reveal_globals.global_conn.cursor()
        cur.execute("drop view "+tabname+ ";")
        cur.close()
        
        # Make a view of name x with second half  T <- T_l
        cur = reveal_globals.global_conn.cursor()
        cur.execute('create view ' + tabname + ' as select * from '+ tabname +'_restore order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[0]) + int(partition_dict[1]/2)) + ' limit ' + str(int(partition_dict[1]) - int(partition_dict[1]/2)) + ';')
        cur.close()
       
        #Run the hidden query on this updated database instance with table T_l
        # new_result = executable.getExecOutput()
        # reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1

        # if(match_oj(Q_E_,new_result) == False):
        if(match_oj(Q_E_) == False):
            Q_E__ = nep_db_minimizer(tabname,Q_E_, int(partition_dict[1]) - int(partition_dict[1]/2), (int(partition_dict[0]) + int(partition_dict[1]/2), int(partition_dict[1]) - int(partition_dict[1]/2)), i )
            
            if Q_E__ == Q_E_ or Q_E__ == True:
                stop = True
                return stop 
            
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
        cur.execute('create view ' + tabname + ' as select * from '+ tabname +'_restore order by ' + reveal_globals.global_pk_dict[tabname] + ' offset ' + str(int(partition_dict[0]) + int(partition_dict[1]/2)) + ' limit ' + str(int(partition_dict[1]) - int(partition_dict[1]/2)) + ';')
        # cur.execute('select count(*) from '+tabname +';')
        # cur.execute('select count(*) from lineitem;')
        # res = cur.fetchone()
        # print(res)
        cur.close()

        #Run the hidden query on this updated database instance with table T_u
        # new_result = executable.getExecOutput()
        # reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
    

        # if(match_oj(Q_E,new_result) == False):
        if(match_oj(Q_E) == False):
            Q_E_ = nep_db_minimizer(tabname,Q_E, int(partition_dict[1]) - int(partition_dict[1]/2), ( int(partition_dict[0]) + int(partition_dict[1]/2), int(partition_dict[1]) - int(partition_dict[1]/2)),i)
            return Q_E_
        else:
            return Q_E

def nep_algorithm(core_relations, Q_E):
    reveal_globals.nep_start_time = time.time()
    # get the size of all the tables
    core_sizes = getCoreSizes(core_relations)

    # STORE STARTING POINT(OFFSET) AND NOOFROWS(LIMIT) FOR EACH TABLE IN FORMAT (offset, limit)
    partition_dict = {}
    for key in core_sizes.keys():
        partition_dict[key] = (0, core_sizes[key])

    # Create the view of all the tables    
    for tabname in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('drop table if exists ' + tabname + ';')
        cur.execute('create view ' + tabname + ' as select * from '+ tabname +'_restore;')
        cur.close()

    # Run the hidden query on the original database instance
    # new_result = executable.getExecOutput()
    # reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
    # print(reveal_globals.global_core_relations)
    
    # if(match_oj(Q_E,new_result) == False):
    if(match_oj(Q_E) == False):
        # NEP may exists
        print("NEP may exists")
        for i in range(len(reveal_globals.global_core_relations)):
            tabname = reveal_globals.global_core_relations[i]
            
            # cur = reveal_globals.global_conn.cursor()
            # cur.execute('select count(*) from ' + tabname + ';')
            # res= cur.fetchone()
            # print(res)
            # cur.close()
            
            Q_E_ = nep_db_minimizer(tabname,Q_E, core_sizes[tabname], partition_dict[tabname],i)
            if Q_E_ == True:
                Q_E = reveal_globals.sem_eq_queries[0]
                
                # Q_E = Q_E_
            #Run the hidden query on the original database instance
            # new_result = executable.getExecOutput()
            # reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
            
            cur = reveal_globals.global_conn.cursor()
            cur.execute('select count(*) from ' + tabname + ';')
            res= cur.fetchone()
            # print(res)
            cur.close()
            
            # for tabname in reveal_globals.global_core_relations:
            cur = reveal_globals.global_conn.cursor()
            # cur.execute('create view ' + tabname + '_xyz as select * from '+ tabname +';')
            cur.execute('drop view if exists ' + tabname + ' cascade ;')
            cur.execute('create view ' + tabname + ' as select * from '+ tabname +'_restore;')
            cur.close()
            
            cur = reveal_globals.global_conn.cursor()
            cur.execute('select count(*) from ' + tabname + ';')
            res= cur.fetchone()
            # print(res)
            cur.close()
            
            
            
            # if(match_oj(Q_E,new_result) == True):
            # if(match_oj(Q_E) == True):
            #     break
            
        
            
        #drop all the views 
        for tabname in reveal_globals.global_core_relations:
            cur = reveal_globals.global_conn.cursor()
            cur.execute('drop view if exists ' + tabname + ';')
            cur.close()
        
        # print("Comparison time:",tim)
        # time.sleep(100)
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
    cur.execute("alter table "+tabname +" rename to " + tabname +"_nep ;")
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

    