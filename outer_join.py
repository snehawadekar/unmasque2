import copy
import reveal_globals
import executable
import nep
import check_nep_oj
import time

def fn():

    # after coming from nep extractor you maynot need this 
    for tabname in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('alter table ' + tabname + '_restore rename to ' + tabname + '2;')
        #The above command will inherently check if tabname1 exists
        cur.execute('drop table ' + tabname + ';')
        cur.execute('alter table ' + tabname + '2 rename to ' + tabname + ';')
        cur.close()

    # aoa join graph edit &&&
    #part of ---  def possible_edge_sequence():
    st = time.time()
    ##########################  prepare list of tables using attributes in join graph---------
    ##########################  new_join graph  =  [table1, table2]
    new_join_graph = [] 
    list_of_tables = []
    for edge in reveal_globals.global_join_graph:
        temp = []
        if len(edge)>2:
            i = 0
            while i<(len(edge))-1:
                temp = []
                cur  =  reveal_globals.global_conn.cursor()
                cur.execute("SELECT COLUMN_NAME ,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME like '"+ str(edge[i])+"' ;")
                tabname = cur.fetchone()
                cur.close()
        
                if tn not in list_of_tables:
                    list_of_tables.append(tabname[1])
                print(edge[i],tabname)
                
                temp.append(tabname)
                cur  =  reveal_globals.global_conn.cursor()
                cur.execute("SELECT COLUMN_NAME ,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME like '"+ str(edge[i+1])+"' ;")
                tabname = cur.fetchone()
                cur.close()
                tn = tabname[1]
                print(tn) #####
                if tn not in list_of_tables:
                    list_of_tables.append(tn)
                print(edge[i+1],tabname)
                temp.append(tabname)
                i = i+1
                new_join_graph.append(temp)   
        else:
            for vertex in edge:
                cur  =  reveal_globals.global_conn.cursor()
                cur.execute("SELECT COLUMN_NAME ,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME like '"+ str(vertex)+"' ;")
                tabname = cur.fetchone()
                cur.close()
                print(vertex,tabname)
                temp.append(tabname)
                tn = tabname[1]
                print(tn) #####
                if tn not in list_of_tables:
                    list_of_tables.append(tn)
            new_join_graph.append(temp)
            
    print(list_of_tables)   
    print(new_join_graph)

    ################## prepare final edge sequence 
    # prepare all valid edge sequence from new_join_graph
    final_edge_seq = []
    queue = []
    for tab in list_of_tables:
        edge_seq = []
        temp_njg = copy.deepcopy(new_join_graph)
        queue.append(tab)
        # table_t = tab
        while len(queue)!=0:
            remove_edge = []
            table_t = queue.pop(0)
            for edge in temp_njg:
                if edge[0][1] == table_t and edge[1][1] != table_t:
                    remove_edge.append(edge)
                    edge_seq.append(edge)
                    queue.append( edge[1][1] )
                elif edge[0][1] != table_t and edge[1][1] == table_t:
                    remove_edge.append(list(reversed(edge)))
                    edge_seq.append(list(reversed(edge)))
                    queue.append( edge[0][1] )

            for i in remove_edge:
                try:
                    temp_njg.remove(i)
                except:
                    temp_njg.remove(list(reversed(i)))
            # print(temp_njg)
        final_edge_seq.append(edge_seq)
    print(final_edge_seq) 
    reveal_globals.final_edge_seq = final_edge_seq

    ################## prepare:  table_attr_dict
    # for each table find a projected attribute which we will check for null values
    table_attr_dict={}
    
    for k in reveal_globals.global_projected_attributes:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("SELECT COLUMN_NAME ,TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME like '"+ str(k)+"' ;")
        tabname = cur.fetchone()
        cur.close()
        if k!='' and tabname[1] not in table_attr_dict.keys():
            # print(k,tabname[1])
            table_attr_dict[tabname[1]] = k
    print(table_attr_dict)

        
    # &&& chnages may be needed 
    # preparing D_1
    for tabname in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute('alter table ' + tabname + ' rename to ' + tabname + '_restore;')
        cur.execute('create table ' + tabname + ' as select * from '  + tabname + '4;')   
        cur.close()

   
    ###############  prepare importance_dict  
    importance_dict = {}
    for edge in new_join_graph:
        # modify d1
        # break join condition on this edge
        tuple1 = edge[0]
        key = tuple1[0]
        table = tuple1[1]
        cur  =  reveal_globals.global_conn.cursor()
        print('Update ' + table + ' set ' + key + '= -(select '+key +' from '+ table +');')
        cur.execute('Update ' + table + ' set ' + key + '= -(select '+key +' from '+ table +');')
        cur.close()

        cur = reveal_globals.global_conn.cursor()
        query = reveal_globals.query1
        cur.execute(query)
        
        colnames = [desc[0] for desc in cur.description]
        res_hq = cur.fetchall()
        # res_hq.append((colnames))
        
        cur.close()
        print(res_hq)
        
        cur = reveal_globals.global_conn.cursor()
        print('Update ' + table + ' set ' + key + '= -(select '+key +' from '+ table +');')
        cur.execute('Update ' + table + ' set ' + key + '= -(select '+key +' from '+ table +');')
        cur.close()

        # make dict of table, attributes: projected val 
        loc = {}
        for table in table_attr_dict.keys():
            loc[ table_attr_dict[table]] =  colnames.index(table_attr_dict[table])
        print(loc)

        ## organizing the hq result in the form of dict
        res_hq_dict = {}
        if len(res_hq)==0:
            for k in loc.keys():
                if k not in res_hq_dict.keys():
                    res_hq_dict[k] = [None]
                else:
                    res_hq_dict[k].append(None)
        else:
            for l in range(len(res_hq)):
                for k in loc.keys():
                    if k not in res_hq_dict.keys():
                        res_hq_dict[k] = [res_hq[l][loc[k]]]
                    else:
                        res_hq_dict[k].append(res_hq[l][loc[k]])
        print(res_hq_dict)

        # importance_dict={}
        # for e in new_join_graph:
        importance_dict[tuple(edge)] = {}
        table1 = edge[0][1] #first table
        table2 = edge[1][1]
        
        p_att_table1 =  None
        p_att_table2 =  None
        att1 = table_attr_dict[table1]
        att2 = table_attr_dict[table2]
        if att1 in res_hq_dict.keys():
            for i in res_hq_dict[att1]:
                if i!= None:
                    p_att_table1 = 10
                    break
        if att2 in res_hq_dict.keys():
            for i in res_hq_dict[att2]:
                if i!= None:
                    p_att_table2 = 10
                    break


        print(p_att_table1,p_att_table2)
        temp1 = ''
        temp2 = ''
        if p_att_table1 == None and p_att_table2== None:
            temp1 = 'l'
            temp2 = 'l'
        elif p_att_table1 == None and p_att_table2!= None:
            temp1 = 'l'
            temp2 = 'h'
        elif p_att_table1 != None and p_att_table2== None:
            temp1 = 'h'
            temp2 = 'l'
        elif p_att_table1 != None and p_att_table2!= None:
            temp1 = 'h'
            temp2 = 'h'


        importance_dict[tuple(edge)][table1] = temp1
        importance_dict[tuple(edge)][table2] = temp2

    print(importance_dict)
    reveal_globals.importance_dict = importance_dict
    # final_edge_seq = possible_edge_sequence()
    set_possible_queries, list_dict_spq = FormulateQueries(reveal_globals.final_edge_seq)
    


    ###################  eliminate semanticamy non-equivalent querie from set_possible_queries
    # --------------
    # we break join-condition for each edge. Run all queries in set possible queries in this mutated 
    # D1mut one at a time and store in result P Q. Run QH on D1mut, store in result HQ. 
    # Eliminate the queries from set possible queries for whom result HQ !=result P Q.
    # this code needs to be finished (27 feb)
    
    sem_eq_queries = []
    sem_eq_listdict =[]
                    
    # print(res_HQ)
    for num in range(0,len(set_possible_queries)):
        poss_q = set_possible_queries[num]
        same = 1
        for edge in new_join_graph:
            # modify d1
            # break join condition on this edge
            tuple1 = edge[0]
            key = tuple1[0]
            table = tuple1[1]
            cur  =  reveal_globals.global_conn.cursor()
            print('Update ' + table + ' set ' + key + '= -(select '+key +' from '+ table +');')
            cur.execute('Update ' + table + ' set ' + key + '= -(select '+key +' from '+ table +');')
            cur.close()
            
            ## result of hidden query
            cur  =  reveal_globals.global_conn.cursor()
            query = reveal_globals.query1
            cur.execute(query)
            res_HQ = cur.fetchall()                
            # colnames = [desc[0] for desc in cur.description]
            # res_hq = cur.fetchall()
            # res_hq.append((colnames))
            cur.close()
            # res_HQ.sort()
            print(res_HQ)
            
            ## result of extracted query
            cur = reveal_globals.global_conn.cursor()
            query = poss_q
            cur.execute(query)
            res_poss_q = cur.fetchall()
            # res_poss_q.sort()
            print(res_poss_q)
            cur.close()

            ##  maybe needs  work
            if len(res_HQ) !=  len(res_poss_q):
                pass
            else:
                ## maybe use the available result comparator techniques
                
                for var in range(len(res_HQ)):
                    print(res_HQ[var] == res_poss_q[var])
                    if ( res_HQ[var] == res_poss_q[var] ) == False:
                        same = 0
                        
            cur = reveal_globals.global_conn.cursor()
            print('Update ' + table + ' set ' + key + '= -(select '+key +' from '+ table +');')
            cur.execute('Update ' + table + ' set ' + key + '= -(select '+key +' from '+ table +');')
            cur.close()

        if same == 1:
            sem_eq_queries.append(poss_q)
            sem_eq_listdict.append(list_dict_spq[num])
          
          
    reveal_globals.sem_eq_queries  = sem_eq_queries
    reveal_globals.sem_eq_listdict = sem_eq_listdict
    temp_seq = reveal_globals.sem_eq_queries
    
    
    reveal_globals.global_oj_time += str(time.time() - st)
    
    

    
    st = time.time()
    reveal_globals.check_nep_again = reveal_globals.nep_flag
    while(reveal_globals.check_nep_again):
        
        reveal_globals.check_nep_again = False
        sem_eq_queries = temp_seq
        temp_seq = []
        # for i in range(0,len(sem_eq_queries)):
        q = sem_eq_queries[0]
        # for tabname in reveal_globals.global_core_relations:
        #     cur = reveal_globals.global_conn.cursor()
        #     cur.execute("drop table if exists "+tabname )
        #     cur.execute("create table " + tabname + " as select * from " + tabname + "_restore;")
        #     cur.close()
        # check_nep_oj.check_nep_oj(q) 
        # nep.match_oj(q)   
        # if reveal_globals.nep_flag:
        qr = nep.nep_algorithm(reveal_globals.global_core_relations, q)
        print(reveal_globals.sem_eq_queries)
        if qr != False:
            for x in reveal_globals.sem_eq_queries:
                temp_seq.append(x)
                
    # print("sem_eq_queries")
    # print(sem_eq_queries)   
    reveal_globals.sem_eq_queries = sem_eq_queries
    
    for tabname in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        # cur.execute('alter table ' + tabname + '1 rename to ' + tabname + '2;')
        # The above command will inherently check if tabname1 exists
        cur.execute('drop table if exists ' + tabname + ';')
        cur.execute('create table ' + tabname + ' as select * from '+ tabname +'_restore;')

        cur.close()
        
    
    # for tabname in reveal_globals.global_core_relations:
    #     cur = reveal_globals.global_conn.cursor()
    #     cur.execute('drop table if exists ' + tabname + ';')
    #     cur.execute('create view ' + tabname + ' as select * from '+ tabname +'_restore;')
    #     cur.close()
    
    # new_result = executable.getExecOutput()
    reveal_globals.nep_flag = False 
    reveal_globals.outer_join_flag = False 
    
    for Q_E in reveal_globals.sem_eq_queries:
        # r = nep.match_oj(Q_E)
        r = check_nep_oj.check_nep_oj( Q_E )
        if reveal_globals.nep_flag == False and reveal_globals.outer_join_flag == False :
            reveal_globals.output1 = Q_E
            break
     
    reveal_globals.global_nep_time += str(time.time() - st)
    print(reveal_globals.output1)
    
    return 


    # once dict is made compare values to null or not null.
    # and prepare importance_dict.
    
    
def FormulateQueries(final_edge_seq):
    # needs to be modified for algebraic predicates
    # and disjunction maybe
    
    
    ### placement of filter predicate #######
    filter_pred_on = []
    filter_pred_where = []
    for fp in reveal_globals.global_filter_predicates_disj:
        cur = reveal_globals.global_conn.cursor()
        # update partsupp set ps_partkey=(select ps_partkey from partsupp)+1;
        print("select " + fp[0][1] + " From " + fp[0][0] +";")
        cur.execute("select " + fp[0][1] + " From " + fp[0][0] +";")
        restore_value = cur.fetchall()
        cur.close()

        cur = reveal_globals.global_conn.cursor()
        # update partsupp set ps_partkey=(select ps_partkey from partsupp)+1;
        print("Update " + fp[0][0] + " Set " + fp[0][1]+" = Null;")
        cur.execute("Update " + fp[0][0] + " Set " + fp[0][1]+" = Null;")
        cur.close()
        cur = reveal_globals.global_conn.cursor()
        query = reveal_globals.query1
        cur.execute(query)
        res_hq = cur.fetchall()
        print(res_hq)
        cur.close()
        if len(res_hq) == 0:
            filter_pred_where.append(fp)
        else:
            filter_pred_on.append(fp)

        print(restore_value[0][0])
        print(reveal_globals.global_attrib_types_dict[(fp[0][0], fp[0][1])])
        if reveal_globals.global_attrib_types_dict[(fp[0][0], fp[0][1])] == 'character' or reveal_globals.global_attrib_types_dict[(fp[0][0], fp[0][1])] == 'date':
            
            cur  =  reveal_globals.global_conn.cursor()
            # update partsupp set ps_partkey = (select ps_partkey from partsupp)+1;
            print("Update " + fp[0][0] + " Set " + fp[0][1]+" = '"+str(restore_value[0][0])+"';")
            cur.execute("Update " + fp[0][0] + " Set " + fp[0][1]+" = '"+str(restore_value[0][0])+"';")
            cur.close()
        else:
            cur  =  reveal_globals.global_conn.cursor()
            # update partsupp set ps_partkey = (select ps_partkey from partsupp)+1;
            print("Update " + fp[0][0] + " Set " + fp[0][1]+" = "+str(restore_value[0][0])+";")
            cur.execute("Update " + fp[0][0] + " Set " + fp[0][1]+" = "+str(restore_value[0][0])+";")
            cur.close()
    print(filter_pred_on, filter_pred_where)

    for fp in reveal_globals.global_filter_aoa:
        cur = reveal_globals.global_conn.cursor()
        # update partsupp set ps_partkey=(select ps_partkey from partsupp)+1;
        print("select " + fp[1] + " From " + fp[0] +";")
        cur.execute("select " + fp[1] + " From " + fp[0] +";")
        restore_value1 = cur.fetchall()
        cur.close()
        
        cur = reveal_globals.global_conn.cursor()
        # update partsupp set ps_partkey=(select ps_partkey from partsupp)+1;
        print("select " + fp[4] + " From " + fp[3] +";")
        cur.execute("select " + fp[4] + " From " + fp[3] +";")
        restore_value2 = cur.fetchall()
        cur.close()

        cur = reveal_globals.global_conn.cursor()
        # update partsupp set ps_partkey=(select ps_partkey from partsupp)+1;
        print("Update " + fp[0] + " Set " + fp[1]+" = Null;")
        cur.execute("Update " + fp[0] + " Set " + fp[1]+" = Null;")
        print("Update " + fp[3] + " Set " + fp[4]+" = Null;")
        cur.execute("Update " + fp[3] + " Set " + fp[4]+" = Null;")
        
        cur.close()
        cur = reveal_globals.global_conn.cursor()
        query = reveal_globals.query1
        cur.execute(query)
        res_hq = cur.fetchall()
        print(res_hq)
        cur.close()
        if len(res_hq) == 0:
            filter_pred_where.append(fp)
        else:
            filter_pred_on.append(fp)

        print(restore_value1[0][0])
        print(reveal_globals.global_attrib_types_dict[(fp[0], fp[1])])
        if reveal_globals.global_attrib_types_dict[(fp[0], fp[1])] == 'character' or reveal_globals.global_attrib_types_dict[(fp[0], fp[1])] == 'date':
            
            cur  =  reveal_globals.global_conn.cursor()
            print("Update " + fp[0] + " Set " + fp[1]+" = '"+str(restore_value1[0][0])+"';")
            cur.execute("Update " + fp[0] + " Set " + fp[1]+" = '"+str(restore_value1[0][0])+"';")
            cur.close()
        else:
            cur  =  reveal_globals.global_conn.cursor()
            print("Update " + fp[0] + " Set " + fp[1]+" = "+str(restore_value1[0][0])+";")
            cur.execute("Update " + fp[0] + " Set " + fp[1]+" = "+str(restore_value1[0][0])+";")
            cur.close()
            
        print(reveal_globals.global_attrib_types_dict[(fp[3], fp[4])])
        if reveal_globals.global_attrib_types_dict[(fp[3], fp[4])] == 'character' or reveal_globals.global_attrib_types_dict[(fp[3], fp[4])] == 'date':
            
            cur  =  reveal_globals.global_conn.cursor()
            print("Update " + fp[3] + " Set " + fp[4]+" = '"+str(restore_value2[0][0])+"';")
            cur.execute("Update " + fp[3] + " Set " + fp[4]+" = '"+str(restore_value2[0][0])+"';")
            cur.close()
        else:
            cur  =  reveal_globals.global_conn.cursor()
            print("Update " + fp[3] + " Set " + fp[4]+" = "+str(restore_value2[0][0])+";")
            cur.execute("Update " + fp[3] + " Set " + fp[4]+" = "+str(restore_value2[0][0])+";")
            cur.close()
            
    print(filter_pred_on, filter_pred_where)

    ########### end of filter predicate placement #############

    set_possible_queries = []
    list_dict_spq =[]
    ##formulate queries function using final_edge_seq
    key_dict = {
        'nation':['n_nationkey','n_regionkey'],
        'region': ['r_regionkey'],
        'supplier': ['s_suppkey', 's_nationkey'],
        'partsupp': ['ps_partkey', 'ps_suppkey'],
        'part': ['p_partkey'],
        'lineitem': ['l_orderkey', 'l_partkey', 'l_suppkey'],
        'orders': ['o_orderkey', 'o_custkey'],
        'customer': ['c_custkey', 'c_nationkey']  
        }

    flat_list = [item for sublist in reveal_globals.global_join_graph for item in sublist]
    keys_of_tables = [*set(flat_list)]
    tables = reveal_globals.global_core_relations

    tables_in_joins = []
    for key in keys_of_tables:
        for tab in tables:
            if key in key_dict[tab] and tab not in tables_in_joins:
                tables_in_joins.append(tab) 

    tables_not_in_joins = []
    for tab in tables:
        if tab not in tables_in_joins:
            tables_not_in_joins.append(tab)

    print(tables_in_joins, tables_not_in_joins)

    importance_dict = reveal_globals.importance_dict
    
    ###############    correction in the select clause  
    for i in range(len(reveal_globals.global_projected_attributes)):
        attrib = reveal_globals.global_projected_attributes[i]
        if attrib in reveal_globals.global_key_attributes and attrib in reveal_globals.global_groupby_attributes:
            if not ('sum' in reveal_globals.global_aggregated_attributes[i][1] or 'count' in reveal_globals.global_aggregated_attributes[i][1]):
                reveal_globals.global_aggregated_attributes[i] = (reveal_globals.global_aggregated_attributes[i][0], '')
    temp_list = copy.deepcopy(reveal_globals.global_groupby_attributes)
    for attrib in temp_list:
        if attrib not in reveal_globals.global_projected_attributes:
            try:
                reveal_globals.global_groupby_attributes.remove(attrib)
            except:
                pass
            continue
        remove_flag = True
        for elt in reveal_globals.global_aggregated_attributes:
            if elt[0] == attrib and (not ('sum' in elt[1] or 'count' in elt[1])):
                remove_flag = False
                break
        if remove_flag == True:
            try:
                reveal_globals.global_groupby_attributes.remove(attrib)
            except:
                pass
    #UPDATE OUTPUTS
    reveal_globals.global_select_op = " "
    first_occur = True
    for i in range(len(reveal_globals.global_projected_attributes)):
        elt = reveal_globals.global_projected_attributes[i]
        reveal_globals.global_output_list.append(copy.deepcopy(elt))
        if reveal_globals.global_aggregated_attributes != [] and reveal_globals.global_aggregated_attributes[i][1] != '' :
            elt = reveal_globals.global_aggregated_attributes[i][1] + '(' + elt + ')'
            if 'count' in reveal_globals.global_aggregated_attributes[i][1]:
                elt = reveal_globals.global_aggregated_attributes[i][1]
            reveal_globals.global_output_list[-1] = copy.deepcopy(elt)
        if elt != reveal_globals.global_projection_names[i] and reveal_globals.global_projection_names[i] != '':
            if elt in keys_of_tables:
            #     make null elt once
                for tab in tables:
                    if elt in key_dict[tab] :
                        tabname = tab
                cur = reveal_globals.global_conn.cursor()
                print("Update " + tabname + " Set " + elt+" = Null;")
                cur.execute("Update " + tabname + " Set " + elt +" = Null;")
                cur.close()
            
            #     run hq
                cur = reveal_globals.global_conn.cursor()
                query = reveal_globals.query1
                cur.execute(query)
                res_hq = cur.fetchall()
                # print(res_hq[0][i])
                cur.close()
            #     if the corresponding value is null elt is the projected column
                if res_hq[0][i] == 'Null':
                    proj_col = elt
                else:
                    for elem in reveal_globals.global_join_graph:
                        if elt in elem:
                            for k in elem:
                                if k != elt:
                                    proj_col = k
                    
            #     make null the key equivalent to elt
            #     again run hq
            #     if the corresponding value is null, the other key is the actual projected column
                elt = proj_col + ' as ' + reveal_globals.global_projection_names[i]
            else:   
                elt = elt + ' as ' + reveal_globals.global_projection_names[i]
            reveal_globals.global_output_list[-1] = copy.deepcopy(reveal_globals.global_projection_names[i])
        if first_occur == True:
            reveal_globals.global_select_op = elt
            first_occur = False
        else:
            reveal_globals.global_select_op = reveal_globals.global_select_op + ", " + elt	


    #### Query formulation begins #############

    for seq in final_edge_seq:
        
        fp_on = copy.deepcopy(filter_pred_on)
        fp_where = copy.deepcopy(filter_pred_where)
        tables_covered = []
    
        d1={}
    
        query =  "Select " + reveal_globals.global_select_op 
        # d1["select"] = reveal_globals.global_select_op
        # handle tables not participating in join
        # d1["from"] = ""
        if len(tables_not_in_joins)!=0:
            query +=" From "
            for tab in tables_not_in_joins:
                query += tab +" , "
                # d1["from"].append(tab +" , ")
        else:
            query +=" From "

        
        flag_first = True
        for edge in seq:
            # steps to determine type of join for edge
            if tuple(edge) in importance_dict.keys():
                imp_t1 = importance_dict[tuple(edge)][edge[0][1]]
                imp_t2 = importance_dict[tuple(edge)][edge[1][1]]
            elif tuple(list(reversed(edge))) in importance_dict.keys():
                imp_t1 = importance_dict[tuple(list(reversed(edge)))][edge[0][1]]
                imp_t2 = importance_dict[tuple(list(reversed(edge)))][edge[1][1]]
            else:
                print("error sneha!!!")

            print(imp_t1 , imp_t2)
            if imp_t1 == 'l' and imp_t2 == 'l':
                type_of_join =  ' Inner Join '
            elif imp_t1 == 'l' and imp_t2 == 'h':
                type_of_join =  ' Right Outer Join '
            elif imp_t1 == 'h' and imp_t2 == 'l':
                type_of_join =  ' Left Outer Join '
            elif imp_t1 == 'h' and imp_t2 == 'h':
                type_of_join =  ' Full Outer Join '

            
            # if join is first join
            if flag_first == True:
                s = str(edge[0][1]) + type_of_join + str(edge[1][1]) +' ON '+ str( edge[0][0] ) +' = '+str( edge[1][0] ) 
                query += s
                # d1["from"]= d1["from"] + s 
                flag_first = False
                # check for filter predicates for both tables
                # append fp to query
                table1 = edge[0][1]
                table2 = edge[1][1]
                
                tables_covered.append(table1)
                tables_covered.append(table2)
                remove_from_fp_on =[]
                
                for fp in fp_on:
                    predicate = ''

                    if type(fp) is tuple : #algebraic predicates
                        if  (fp[0] == table1 and fp[3] == table2) or (fp[0] == table2 and fp[3] == table1):
                            print("aoa pred")
                            predicate = fp[1] + " " + str(fp[2]) + " " + str(fp[4]) + " "
                            query +=" and ("+ predicate+")"
                            # d1["from"]+=" and ("+ predicate+")"
                            remove_from_fp_on.append(fp)
                        elif fp[0] == fp[3] and (fp[0] == table1  or fp[0] == table2 ):
                            predicate = fp[1] + " " + str(fp[2]) + " " + str(fp[4]) + " "
                            query +=" and ("+ predicate+")"
                            # d1["from"]+=" and ("+ predicate+")"
                            remove_from_fp_on.append(fp)
                    
                    elif len(fp) == 1:        #normal filter predicate
                                                
                        if fp[0][0] == table1 or fp[0][0] == table2:
                            
                            elt = fp[0]
                            if elt[2].strip() == 'range':
                                if '-' in str(elt[4]):
                                    predicate += elt[1] + " between "  + str(elt[3]) + " and " + str(elt[4])
                                else:
                                    predicate += elt[1] + " between "  + " '" + str(elt[3]) + "'" + " and " + " '" + str(elt[4]) + "'"
                            elif elt[2].strip() == '>=':
                                if '-' in str(elt[3]):
                                    predicate += elt[1] + " " + str(elt[2]) + " '" + str(elt[3]) + "' "
                                else:
                                    predicate += elt[1] + " " + str(elt[2]) + " " + str(elt[3])
                            elif '<>' in elt[2] :
                                print(type(elt[3]))
                                if "<class 'datetime.date'>"==str(type(elt[3])):
                                    predicate += elt[1] + " "+ str(elt[2]) + " '" + str(elt[3])+"' "
                                elif type(elt[3]) == str:
                                    predicate += elt[1] + " "+ str(elt[2]) + " '" + elt[3]+"' "
                                else:
                                    predicate += elt[1] + " "+ str(elt[2]) + " " + str(elt[3])

                            elif 'equal' in elt[2] or 'like' in elt[2].lower() or '-' in str(elt[4]):
                                predicate += elt[1] + " " + str(elt[2]).replace('equal', '=') + " '" + str(elt[4]) + "'"
                            
                            else:
                                predicate += elt[1] + ' ' + str(elt[2]) + ' ' + str(elt[4])
                                # predicate +=''
                            remove_from_fp_on.append(fp)
                            
                    elif len(fp) > 1: # disjunction / OR predicate
                        include_flag =  True
                        for element in fp:
                            if element[0] not in tables_covered :
                                include_flag = False
                        print("include flag == ", include_flag)
                        
                        if include_flag:
                            flag = True
                            for ele in fp :
                                
                                if flag:
                                    if 'equal' in ele[2] or 'like' in ele[2].lower() or '-' in str(ele[4]):
                                        predicate += ele[1] + " " + str(ele[2]).replace('equal', '=') + " '" + str(ele[4]) + "'"
                                    elif ele[2].strip() == '>=':
                                        predicate += ele[1] + " " + str(ele[2]) + " " + str(ele[3]) + " "                                       
                                    
                                    else:
                                        predicate += ele[1] + " " + str(ele[2]) + " " + str(ele[4]) + " "
                                    flag = False
                                else:
                                    if 'equal' in ele[2] or 'like' in ele[2].lower() or '-' in str(ele[4]):
                                        predicate += " OR " + ele[1] + " " + str(ele[2]).replace('equal', '=') + " '" + str(ele[4]) + "'"
                                    elif ele[2].strip() == '>=':
                                        predicate += " OR " +  ele[1] + " " + str(ele[2]) + " " + str(ele[3]) + " "                                       
                                    else:
                                        predicate += " OR " + ele[1] + " " + str(ele[2]) + " " + str(ele[4]) + " "
                            remove_from_fp_on.append(fp)  
                    
                    # after processing the predicate include it in query
                    if predicate != "":
                        query +=" and ("+ predicate+")"
                        # d1["from"] +=" and ("+ predicate+")"
                for elem in remove_from_fp_on:
                    fp_on.remove(elem)
            # second join onwards
            else:
                # cases where joins between more than 2 tables exist
                query+= ' ' + type_of_join + str(edge[1][1]) +' ON '+ str( edge[0][0] ) +' = '+str( edge[1][0] ) 
                tables_covered.append(edge[1][1])
                predicate = ''
                remove_from_fp_on = []
                for fp in fp_on:
                    if type(fp) is list and len(fp) > 1 : # disjunction / OR predicate
                        print (" or in ON ")
                        include_flag =  True
                        for element in fp:
                            if element[0] not in tables_covered :
                                include_flag = False
                        
                        if include_flag:
                            flag = True
                            for ele in fp :
                                if flag:
                                    if 'equal' in ele[2] or 'like' in ele[2].lower() or '-' in str(ele[4]):
                                        predicate += ele[1] + " " + str(ele[2]).replace('equal', '=') + " '" + str(ele[4]) + "'"
                                    elif ele[2].strip() == '>=':
                                        predicate += ele[1] + " " + str(ele[2]) + " " + str(ele[3]) + " "                                       
                                    else:
                                        predicate += ele[1] + " " + str(ele[2]) + " " + str(ele[4]) + " "
                                    flag = False
                                else:
                                    if 'equal' in ele[2] or 'like' in ele[2].lower() or '-' in str(ele[4]):
                                        predicate += " OR "+ele[1] + " " + str(ele[2]).replace('equal', '=') + " '" + str(ele[4]) + "'"
                                    elif ele[2].strip() == '>=':
                                        predicate += " OR "+ ele[1] + " " + str(ele[2]) + " " + str(ele[3]) + " " 
                                    else:
                                        predicate += " OR "+ele[1] + " " + str(ele[2]) + " " + str(ele[4]) + " "
                            remove_from_fp_on.append(fp)
                            
                        if predicate != "":
                            query +=" and ("+ predicate+")"
                            
                    elif type(fp) is tuple :
                        if (fp[0] == edge[1][1] or fp[3] == edge[1][1]): #for algebraic predicates
                            print("aoa pred")
                            predicate = fp[1] + " " + str(fp[2]) + " " + str(fp[4]) + " "
                            query +=" and ("+ predicate+")"
                            remove_from_fp_on.append(fp)  
                        elif fp[0] == fp[3] and (fp[0] == edge[1][1]  or fp[0] == table2 ):
                            predicate = fp[1] + " " + str(fp[2]) + " " + str(fp[4]) + " "
                            query +=" and ("+ predicate+")"
                            remove_from_fp_on.append(fp)
                                          
                    elif fp[0][0] == edge[1][1]:  # normal filter predicate 
                        predicate = ''
                        elt = fp[0]
                        if elt[2].strip() == 'range':
                            if '-' in str(elt[4]):
                                predicate = elt[1] + " between "  + str(elt[3]) + " and " + str(elt[4])
                            else:
                                predicate = elt[1] + " between "  + " '" + str(elt[3]) + "'" + " and " + " '" + str(elt[4]) + "'"
                        elif elt[2].strip() == '>=':
                            if '-' in str(elt[3]):
                                predicate = elt[1] + " " + str(elt[2]) + " '" + str(elt[3]) + "' "
                            else:
                                predicate = elt[1] + " " + str(elt[2]) + " " + str(elt[3])
                        elif '<>' in elt[2] :
                            print(type(elt[3]))
                            if "<class 'datetime.date'>"==str(type(elt[3])):
                                predicate += elt[1] + " "+ str(elt[2]) + " '" + str(elt[3]) +"' "
                            elif type(elt[3]) == str:
                                predicate += elt[1] + " "+ str(elt[2]) + " '" + elt[3]+"' "
                            else:
                                predicate += elt[1] + " "+ str(elt[2]) + " " + str(elt[3])

                        
                        elif 'equal' in elt[2] or 'like' in elt[2].lower() or '-' in str(elt[4]):
                            predicate = elt[1] + " " + str(elt[2]).replace('equal', '=') + " '" + str(elt[4]) + "'"
                        else:
                            predicate = elt[1] + ' ' + str(elt[2]) + ' ' + str(elt[4])
                        query +=" and "+ predicate
                        remove_from_fp_on.append(fp)
                        # fp_on.remove(fp)
                for elem in remove_from_fp_on:
                    fp_on.remove(elem)       
                
        # add other components of the query
        # + where clause
        # + group by, order by, limit
        reveal_globals.global_where_op = ''

        
        for elt in fp_where:
            if type(elt) is tuple:
                print("aoa pred")
                predicate = elt[1] + " " + str(elt[2]) + " " + str(elt[4]) + " "
         
                
            
            elif len(elt) == 1:
                elt = elt[0]
                
                predicate = ''
                if elt[2].strip() == 'range': 
                    if "<class 'datetime.date'>"==str(type(elt[4])): #make changes for date in here
                        predicate = elt[1] + " between date"  + " '" + str(elt[3]) + "'" + " and date" + " '" + str(elt[4]) + "'"
                    # print(type(elt[4]))
                    elif '-' in str(elt[4]):
                        predicate = elt[1] + " between "  + str(elt[3]) + " and " + str(elt[4])
                    else:
                        predicate = elt[1] + " between "  + " '" + str(elt[3]) + "'" + " and " + " '" + str(elt[4]) + "'"
                elif elt[2].strip() == '>=':
                    if '-' in str(elt[3]):
                        predicate = elt[1] + " " + str(elt[2]) + " '" + str(elt[3]) + "' "
                    else:
                        predicate = elt[1] + " " + str(elt[2]) + " " + str(elt[3])
                
                elif '<>' in elt[2] :
                    print(type(elt[3]))
                    if "<class 'datetime.date'>"==str(type(elt[3])):
                        predicate += elt[1] + " "+ str(elt[2]) + " '" + str(elt[3])+"' "
                    elif type(elt[3]) == str:
                        predicate += elt[1] + " "+ str(elt[2]) + " '" + elt[3]+"' "
                    else:
                        predicate += elt[1] + " "+ str(elt[2]) + " " + str(elt[3])

                
                elif 'equal' in elt[2] or 'like' in elt[2].lower() or '-' in str(elt[4]):
                    predicate = elt[1] + " " + str(elt[2]).replace('equal', '=') + " '" + str(elt[4]) + "'"
                else:
                    predicate = elt[1] + ' ' + str(elt[2]) + ' ' + str(elt[4])
                    
                
            else:
                flg = True
                predicate = '('
                for disj in elt:
                    if flg == False:
                        predicate += " or "
                    
                    if disj[2].strip() == 'range': 
                        if "<class 'datetime.date'>"==str(type(disj[4])): #make changes for date in here
                            predicate += disj[1] + " between date"  + " '" + str(disj[3]) + "'" + " and date" + " '" + str(disj[4]) + "'"
                        # print(type(elt[4]))
                        elif '-' in str(disj[4]):
                            predicate += disj[1] + " between "  + str(disj[3]) + " and " + str(disj[4])
                        else:
                            predicate += disj[1] + " between "  + " '" + str(disj[3]) + "'" + " and " + " '" + str(disj[4]) + "'"
                    elif disj[2].strip() == '>=':
                        if '-' in str(disj[3]):
                            predicate += disj[1] + " " + str(disj[2]) + " '" + str(disj[3]) + "' "
                        else:
                            predicate += disj[1] + " " + str(disj[2]) + " " + str(disj[3])
                    elif 'equal' in disj[2] or 'like' in disj[2].lower() or '-' in str(disj[4]):
                        predicate += disj[1] + " " + str(disj[2]).replace('equal', '=') + " '" + str(disj[4]) + "'"
                    else:
                        predicate += disj[1] + ' ' + str(disj[2]) + ' ' + str(disj[4])
                    flg = False    
                predicate += ')'
            if reveal_globals.global_where_op == '':
                reveal_globals.global_where_op = predicate
            else:
                reveal_globals.global_where_op = reveal_globals.global_where_op + " and " + predicate  

        # d1["where"] = reveal_globals.global_where_op
        #assemble the rest of the query
        if reveal_globals.global_where_op != '':
            query = query + " Where " + reveal_globals.global_where_op
        if reveal_globals.global_groupby_op != '':
            query = query + " Group By " + reveal_globals.global_groupby_op
        if reveal_globals.global_orderby_op != '':
            query = query + " Order By " + reveal_globals.global_orderby_op
        if reveal_globals.global_limit_op  != '':
            query = query + " Limit " + reveal_globals.global_limit_op 


        print(query)
        print("+++++++++++++++++++++")
        set_possible_queries.append(query)
        list_dict_spq.append(d1)

    return set_possible_queries, list_dict_spq


    





