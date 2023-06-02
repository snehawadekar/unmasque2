import reveal_globals

def check():
    try:
        cur = reveal_globals.global_conn.cursor()
        query=reveal_globals.query1
        print("query=",query)
        reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
        cur.execute(query)
        # res = cur.fetchall() #fetchone always return a tuple whereas fetchall return list
        while(True):
            res = cur.fetchmany(100000)
            if not res:
                return 
            else:
                for row in res:
                    if None in row:
                        reveal_globals.outer_join_flag = True
                        return 
        cur.close()
       
    except Exception as error:
        # reveal_globals.error='Unmasque Error: \n Executable could not be run. Error: ' +  dict(error.args[0])['M']
        print('Executable could not be run. Error: ' + str(error))
        raise error
    return 