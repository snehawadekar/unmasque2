import reveal_globals

def check_nep_oj(query):
    reveal_globals.outer_join_flag = False
    for tabname in reveal_globals.global_core_relations:
        cur = reveal_globals.global_conn.cursor()
        cur.execute("drop table if exists "+tabname )
        cur.execute("create table " + tabname + " as select * from " + tabname + "_restore;")
        cur.close()

    try:
        cur = reveal_globals.global_conn.cursor()
        hidden_query=reveal_globals.query1
        # extracted_query = reveal_globals.output1
        extracted_query = query
        print("extracted_query=",query)
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
        
        temp3=[] # outer join
        for element in res_hq:
            if element not in res_eq:
                temp3.append(element)
                reveal_globals.outer_join_flag = True
                break
                
                
        temp4=[] # nep 
        for element in res_eq:
            if element not in res_hq:
                temp4.append(element)
                reveal_globals.nep_flag = True
                break
                
        
         
                
        # cur = reveal_globals.global_conn.cursor()
        # cur.execute("SELECT * FROM ("+ hidden_query + "\n  EXCEPT " + extracted_query +") as foo;")
        # res_1 = cur.fetchall() #fetchone always return a tuple whereas fetchall return list
        # cur.close()
        # print(len(res_1))
        
        # cur = reveal_globals.global_conn.cursor()
        # cur.execute("SELECT * FROM ("+ extracted_query + "\n  EXCEPT " + hidden_query +") as foo;")
        # res_1 = cur.fetchall() #fetchone always return a tuple whereas fetchall return list
        # cur.close()
        # print(len(res_1))
              
       
    except Exception as error:
        # reveal_globals.error='Unmasque Error: \n Executable could not be run. Error: ' +  dict(error.args[0])['M']
        print('Executable could not be run. Error: ' + str(error))
        raise error
    return 