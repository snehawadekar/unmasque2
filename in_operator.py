import executable
import check_nullfree
import copy
import reveal_globals
import os
import where_clause
import time
import pandas as pd

potential_in_attrib = []
filter_table_list = {}
new_filter_list = []
temp_list = []


def extract(level,att,new_list):
    
    attrib_types_dict = {}
    for entry in reveal_globals.global_attrib_types:
        attrib_types_dict[(entry[0], entry[1])] = entry[2]
        
    local_start=time.time()
    
    for tabname in reveal_globals.global_core_relations:
        cur=reveal_globals.global_conn.cursor()
        query1='truncate table '+ tabname+' ;'
        cur.execute(query1)
        condition = "true "
        
        if tabname in filter_table_list.keys():
            condition="true "
            for filters in filter_table_list[tabname]:
                if filters==att:
                    if 'date' in attrib_types_dict[(filters[0], filters[1])]:
                        condition=condition+"and ("+filters[1]+" < date \'"+str(filters[3])+"\' or "+filters[1]+ " > date\'"+str(filters[4])+"\') "
                    elif ('int' in attrib_types_dict[(filters[0], filters[1])] or 'numeric' in attrib_types_dict[(filters[0],filters[1])]):
                        condition=condition+"and ("+filters[1]+" < "+str(filters[3])+" or "+filters[1]+ " > "+str(filters[4])+") "            
                    else:
                        condition=condition+"and ("+filters[1]+" < '"+str(filters[3])+"' or "+filters[1]+ " > '"+str(filters[4])+"') "
                    continue
                else:
                    if ('int' in attrib_types_dict[(filters[0], filters[1])] or 'numeric' in attrib_types_dict[(filters[0],filters[1])]):    
                        condition=condition+"and ("+filters[1]+" >= "+str(filters[3])+" and "+filters[1]+ " <="+str(filters[4])+") "
                    elif 'date' in attrib_types_dict[(filters[0], filters[1])] :
                        condition=condition+"and ("+filters[1]+" >= date '"+str(filters[3])+"' and "+filters[1]+ " <= date '"+str(filters[4])+"') "
                    else:
                        condition=condition+"and ("+filters[1]+" >= '"+str(filters[3])+"' and "+filters[1]+ " <= '"+str(filters[4])+" ') "
            for filters in new_list:
                if filters[0] == tabname:
                    if 'date' in attrib_types_dict[(filters[0], filters[1])]:
                        condition=condition+"and ("+filters[1]+" < date \'"+str(filters[3])+"\' or "+filters[1]+ " > date\'"+str(filters[4])+"\') "
                    elif ('int' in attrib_types_dict[(filters[0], filters[1])] or 'numeric' in attrib_types_dict[(filters[0],filters[1])]):
                        condition=condition+"and ("+filters[1]+" < "+str(filters[3])+" or "+filters[1]+ " > "+str(filters[4])+") "            
                    else:
                        condition=condition+"and ("+filters[1]+" < '"+str(filters[3])+"' or "+filters[1]+ " > '"+str(filters[4])+"') "

        

        query2="Insert into " + tabname + " (select * from " + tabname + "_restore where "+condition+");"
        temp_query_time=time.time()
        print("runnning query :"+query2)                   
        cur.execute(query2)
        cur.close()
        
        # cur = reveal_globals.global_conn.cursor()
        # cur.execute("Select count(*) from " + tabname + ";")
        # res = cur.fetchall()
        # print(res)
        # cur.close()
        print("temp query time: " + str(time.time()-temp_query_time))

    # res=executable.getExecOutput()
    print(check_nullfree.getExecOutput())
    # if len(res) > 1 and check_nullfree.getExecOutput():
    if check_nullfree.getExecOutput():
        print("possible IN at:" + att[0]+','+att[1])
        # for tabname in reveal_globals.global_core_relations:
        #     if level == 1:
        #         pass
        #         cur.execute("alter table "+tabname+"1 rename to "+tabname+"100 ;")
        #     else:
        #         cur.execute("drop table "+tabname+"1 ;")
            
        temp_min_start=time.time()
        if(in_dbmin (reveal_globals.global_core_relations)):
        # if (view_minimizer.reduce_Database_Instance_cs_fail(reveal_globals.global_core_relations)):
        # if (db_minimizer.reduce_Database_Instance(reveal_globals.global_core_relations)):
        # if (reduce_Database_Instance(reveal_globals.global_core_relations)):
            temp = " "
            print("temp min time:" + str(time.time() - temp_min_start))
            reveal_globals.global_filter_predicates = where_clause.get_filter_predicates()
            print(reveal_globals.global_filter_predicates)
            fp = reveal_globals.global_filter_predicates
            new_temp=[]
    
            for e2 in fp:
                include_flag = True
                for ele in (reveal_globals.global_filter_aeq ):
                    if ele[1] == e2[1] and e2[2] == '=':
                        include_flag = False
                        
                for ele in (reveal_globals.global_filter_aoa ):
                    if ele[1] == e2[1] or ele[4] == e2[1]:
                        include_flag = False
                if include_flag:
                    new_temp.append(e2)
            
            reveal_globals.global_filter_predicates = new_temp
                    
            for item in reveal_globals.global_filter_predicates:
                if item not in potential_in_attrib and item[0][1] not in reveal_globals.global_key_attributes:
                    temp=item
            
            
            if temp != " ":
                new_list.append(temp)
                temp_list.append(temp)
                if temp[0] not in  filter_table_list.keys():
                    filter_table_list[temp[0]]=[temp]
                # else:
                #     filter_table_list[temp[0]].append(temp)
                print("new condition: "+str(temp))
                
                cur = reveal_globals.global_conn.cursor()
                query = "select data_type from information_schema.columns where table_name ='"+temp[0]+"' and column_name = '" + temp[1] + "' order by ordinal_position; "
                cur.execute(query)
                res=cur.fetchall()
                for row in res:
                    attrib_types_dict[(temp[0],temp[1])]=row[0]
                print("local disjunction time: "+str(time.time()-local_start))
                cur.close()
                extract(level+1,att,new_list)
         
        else:
            print('error with minimizer')
            cur.close()
    else:
        print('No disjunction at :  '+ att[0]+','+att[1])
        # if level >1:
        #     for tabname in reveal_globals.global_core_relations:
        #         cur.execute("drop table "+tabname+"1 ;")
        #         cur.execute("alter table "+tabname+"100 rename to "+tabname+ "1 ;" )
        # cur.close()       

def in_extract(level=1):
    global_start=time.time()
    print(reveal_globals.global_filter_predicates)
    #load all data for level1
    count_in=0
    cur = reveal_globals.global_conn.cursor()
    for tabname in reveal_globals.global_core_relations:
        cur.execute("Alter table " + tabname + "4 rename to " + tabname + "_in_temp;")
    cur.close()
    
    
    for att in reveal_globals.global_filter_predicates:
        try:
            filter_table_list[att[0]].append(att)
        except:
            filter_table_list[att[0]]=[att]
        # if att[1] not in reveal_globals.global_key_attributes:
        potential_in_attrib.append(att)

    for att in potential_in_attrib:
        temp_list.clear()
        temp_list.append(att)
        cur = reveal_globals.global_conn.cursor()
        extract(level,att,[])
        new_filter_list.append(copy.deepcopy(temp_list))
    print(new_filter_list)
    print('Disjunction Time: '+str(time.time() - global_start))
    reveal_globals.global_disj_time = time.time() -global_start
    reveal_globals.global_filter_predicates_disj = copy.deepcopy(new_filter_list)
    cur = reveal_globals.global_conn.cursor()
    for tabname in reveal_globals.global_core_relations:
        cur.execute('drop table if exists ' + tabname + '4;')
        cur.execute("Alter table " + tabname + "_in_temp rename to " + tabname + "4;")
    cur.close()
    
    return
        
   

def in_dbmin(core_relations, method = 'binary partition', max_no_of_rows = 1, executable_path = ""):
    reveal_globals.local_other_info_dict = {}
    #Perform sampling
    #print(core_relations, reveal_globals.global_sample_size_percent, reveal_globals.global_sample_threshold, reveal_globals.global_max_sample_iter,"++++SAMPLING++++++++")
    # core_sizes = sample_Database_Instance(core_relations, reveal_globals.global_sample_size_percent, reveal_globals.global_sample_threshold, reveal_globals.global_max_sample_iter)
    # print("sneha here")
    # exit(0)

    # core_sizes = getCoreSizes(core_relations)
    core_sizes =  reveal_globals.global_core_sizes
    start_time=time.time()
    # print("YES1")

        

    print("xhcbhcb")
    '''
    cur = reveal_globals.global_conn.cursor()
    cur.execute("set synchronize_seqscans = 'OFF';")
    cur.close()
    '''
    for tabname in reveal_globals.global_core_relations:
        
        cur = reveal_globals.global_conn.cursor()
        cur.execute('drop table if exists ' + tabname + '_in;')
        cur.execute('alter table ' + tabname + ' rename to '+tabname+'_in ;')
        # cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1;')
        cur.close()

        cur = reveal_globals.global_conn.cursor()
        cur.execute('select min(ctid), max(ctid) from '+ tabname+'_in; ')
        rctid =cur.fetchone()
        cur.close()
        min_ctid = rctid[0]
        # print(min_ctid)
        min_ctid2 = min_ctid.split(",")
        start_page = int(min_ctid2[0][1:])
        max_ctid = rctid[1]
        # print(max_ctid)
        max_ctid2 = max_ctid.split(",")
        end_page = int(max_ctid2[0][1:])

        # cur = reveal_globals.global_conn.cursor()
        # cur.execute('select max(ctid) from '+ tabname+'1; ')
        # max_ctid =cur.fetchone()
        # cur.close()
        # max_ctid = max_ctid[0]
        # print(max_ctid)
        # max_ctid2 = max_ctid.split(",")
        # end_page = int(max_ctid2[0][1:])


        start_ctid = min_ctid
        end_ctid = max_ctid
        # print("start_page= ",start_page, "end page= ",end_page )
        while (start_page < end_page-1):
            mid_page=int((start_page + end_page)/2)
            mid_ctid1 = "(" + str(mid_page) + ",1)"
            mid_ctid2 = "(" + str(mid_page) + ",2)"

            
            cur = reveal_globals.global_conn.cursor()
            # cur.execute('drop view '+ tabname + ';')
            cur.execute("create view " + tabname + " as select * from "+ tabname +"_in where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(mid_ctid1) + "'  ; ")
            cur.close()

            #Run query and analyze the result now
            
            # reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
            # cur = reveal_globals.global_conn.cursor()
            # cur.execute('drop view '+ tabname + ';')
            # cur.close()

            # new_result = executable.getExecOutput()
            # new_result_flag = getExecOutput()

            # # if len(new_result) <= 1:
            # if new_result_flag == False: 
            # 	#Take the lower half
            # 	start_ctid = mid_ctid2
            # else:
            # 	#Take the upper half
            # 	end_ctid = mid_ctid1
            # # start_page=start_ctid[0]

            #UN+nf
            if check_nullfree.getExecOutput() == False:
                #Take the lower half
                start_ctid = mid_ctid2
            else:
                #Take the upper half
                end_ctid = mid_ctid1

            cur = reveal_globals.global_conn.cursor()
            cur.execute('drop view '+ tabname + ';')
            cur.close()

            start_ctid2 = start_ctid.split(",")
            start_page = int(start_ctid2[0][1:])
            end_ctid2 = end_ctid.split(",")
            end_page = int(end_ctid2[0][1:])
            # print("start_page= ",start_page, "end page= ",end_page )
            # print(start_ctid, end_ctid)
        cur = reveal_globals.global_conn.cursor()

        # cur.execute('drop view '+ tabname + ';')
        # print("create table " + tabname + " as select * from "+ tabname +"_in where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
        cur.execute("create table " + tabname + " as select * from "+ tabname +"_in where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
        # cur.execute('drop table ' + tabname + '1 ;')
        cur.close()
        cur = reveal_globals.global_conn.cursor()
        cur.execute("select count(*) from " + tabname + ";")
        size = int(cur.fetchone()[0])
        cur.close()
        core_sizes[tabname] = size
        print("REMAINING TABLE SIZE", core_sizes[tabname])

        # print(start_ctid, end_ctid)


        
        while(int(core_sizes[tabname]) > max_no_of_rows):
            cur = reveal_globals.global_conn.cursor()
            cur.execute('alter table ' + tabname + ' rename to '+tabname+'1 ;')
            # cur.execute('create view ' + tabname + ' as select * from '+ tabname +'1;')
            cur.close()
            
            cur = reveal_globals.global_conn.cursor()
            cur.execute('select min(ctid) from '+ tabname+'1; ')
            min_ctid =cur.fetchone()
            cur.close()
            min_ctid = min_ctid[0]
            min_ctid= min_ctid[1:-1]
            min_ctid2 = min_ctid.split(",")
            # print(min_ctid2)
            start_row = int(min_ctid2[1])
            start_page = int(min_ctid2[0])

            cur = reveal_globals.global_conn.cursor()
            cur.execute('select max(ctid) from '+ tabname+'1; ')
            max_ctid =cur.fetchone()
            cur.close()
            max_ctid = max_ctid[0]
            max_ctid =max_ctid[1:-1]
            max_ctid2 = max_ctid.split(",")
            # print(max_ctid2)
            end_row = int(max_ctid2[1])
            end_page = int(max_ctid2[0])
            # print("start_row= ",start_row, "end_row = ",end_row ,"#########")

            # mid_page=int((start_page + end_page)/2)
            start_ctid = "(" + str(start_page) + "," + str(start_row) + ")"
            end_ctid = "(" + str(end_page) + "," + str(end_row) + ")"

            # if start_page!= end_page:
            # 	#wish to know last row in start page
            # 	mid_ctid1="(" + str(end_page) + "," + str(0) + ")"
            # 	mid_ctid2="(" + str(0) + "," + str(mid_row+1) + ")"
            mid_row=int( core_sizes[tabname] /2)

            mid_ctid1="(" + str(0) + "," + str(mid_row) + ")"
            mid_ctid2="(" + str(0) + "," + str(mid_row+1) + ")"

            
            cur = reveal_globals.global_conn.cursor()
            # cur.execute('drop view '+ tabname + ';')
            cur.execute("create view " + tabname + " as select * from "+ tabname +"1 where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(mid_ctid1) + "'  ; ")
            cur.close()

            #Run query and analyze the result now
            
            # reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
            # cur = reveal_globals.global_conn.cursor()
            # cur.execute('drop view '+ tabname + ';')
            # cur.close()

            # new_result = executable.getExecOutput()
            # new_result_flag = getExecOutput()

            # # if len(new_result) <= 1:
            # if new_result_flag == False: 
            # 	#Take the lower half
            # 	start_ctid = mid_ctid2
            # else:
            # 	#Take the upper half
            # 	end_ctid = mid_ctid1
            # # start_page=start_ctid[0]

            #UN+nf
            if check_nullfree.getExecOutput() == False:
                #Take the lower half
                start_ctid = mid_ctid2
            else:
                #Take the upper half
                end_ctid = mid_ctid1

            cur = reveal_globals.global_conn.cursor()
            cur.execute('drop view '+ tabname + ';')
            cur.close()



            cur = reveal_globals.global_conn.cursor()
            cur.execute("create table " + tabname + " as select * from "+ tabname +"1 where ctid >= '" + str(start_ctid) + "' and ctid <= '" + str(end_ctid) + "'  ; ")
            cur.execute('drop table ' + tabname + '1 ;')
            cur.close()
            cur = reveal_globals.global_conn.cursor()
            cur.execute("select count(*) from " + tabname + ";")
            size = int(cur.fetchone()[0])
            cur.close()
            core_sizes[tabname] = size
            print("REMAINING TABLE SIZE", core_sizes[tabname])

        #SANITY CHECK
        # new_result = executable.getExecOutput()
        # new_result_flag = getExecOutput()

        

    #WRITE TO Reduced Data Directory
    #check for data directory existence, if not exists , create it
    if not os.path.exists(reveal_globals.global_reduced_data_path):
        os.makedirs(reveal_globals.global_reduced_data_path)
    for tabname in core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("select * from " + tabname + ";")
        res=cur.fetchall()
        # cur.execute(" COPY " + tabname + " to " + "'" + reveal_globals.global_reduced_data_path + tabname + ".csv' " + "delimiter ',' csv header;")
        cur.close()
        cur = reveal_globals.global_conn.cursor()
        cur.execute('drop table if exists '+ tabname + "4 ;")
        cur.execute("create table " + tabname + "4 as select * from " + tabname + ";")
        cur.close()
        print(tabname, "==", res)
    #SANITY CHECK 
    # new_result = executable.getExecOutput()
    # if len(new_result) <= 1:
    # 	print("Error: Query out of extractable domain\n")
    # 	return False
    if check_nullfree.getExecOutput() == False:
        print("Error: Query out of extractable domain\n")
        return False

    #populate screen data
    #POPULATE MIN INSTANCE DICT
    # reveal_globals.view_min_time=time.time()-start_time
    conn=reveal_globals.global_conn
    for tabname in reveal_globals.global_core_relations:
        reveal_globals.global_min_instance_dict[tabname] = []
        sql_query = pd.read_sql_query("select * from "+tabname+";",conn)
        df = pd.DataFrame(sql_query)
        reveal_globals.global_min_instance_dict[tabname].append(tuple(df.columns))
        for index, row in df.iterrows():
            reveal_globals.global_min_instance_dict[tabname].append(tuple(row))
    #populate other data
    new_result=executable.getExecOutput()
    reveal_globals.global_result_dict['min'] = copy.deepcopy(new_result)
    reveal_globals.local_other_info_dict['Result Cardinality'] = str(len(new_result) - 1)
    reveal_globals.global_other_info_dict['min'] = copy.deepcopy(reveal_globals.local_other_info_dict)
    return True	
