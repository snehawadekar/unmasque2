import reveal_globals
import time

# HAsh based result comparator
def match(Q_E):
        
    st=time.time()
    cur  = reveal_globals.global_conn.cursor()
    cur.execute("create view r_e as "+ Q_E)
    cur.close()
    print("line 11  --   ", time.time() - st)
    
    cur  = reveal_globals.global_conn.cursor()
    cur.execute("Select count(*) from r_e")
    res1 = cur.fetchone()[0]
    cur.close()
    print("line 17  --   ",time.time() - st)
 
 
    query=reveal_globals.query1
    #print("query=",query)
    cur  = reveal_globals.global_conn.cursor()
    reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
    cur.execute(query)
    res = cur.fetchall() #fetchone always return a tuple whereas fetchall return list
    #print(res)
    colnames = [desc[0] for desc in cur.description] 
    cur.close()
    print("line 29  --   ", time.time() - st)
    
    if(res1 != (len(res))):
        return False
    
    result = []
    result.append(tuple(colnames))
    #print(result) it will print 8 times for from clause 
        #it will append attribute name in the result
        
    cur = reveal_globals.global_conn.cursor()
    cur.execute('Create unlogged table r_h (like r_e);')
    cur.close()
    print("line 42  --   ", time.time() - st)
 
    # Header of r_h
    t = result[0]
    t1 = '(' + t[0]
    for i in range(1,len(t)):
        t1 += ', ' + t[i]
    t1 += ')'    
        
    if len(res) == 1 and len(res[0]) == 1:
        if res is not None:
            for row in res:
                #CHECK IF THE WHOLE ROW IN NONE (SPJA Case)
                nullrow = True
                for val in row:
                    if val != None:
                        nullrow = False
                        break
                if nullrow == True:
                    continue
                temp = []
                for val in row:
                    temp.append(str(val))
                ins = (tuple(temp))
                cur = reveal_globals.global_conn.cursor()
                cur.execute('INSERT INTO r_h'+str(t1)+' VALUES ('+str(ins[0])+'); ')
                cur.close()
        
    else:
        if res is not None:
            for row in res:
                #CHECK IF THE WHOLE ROW IN NONE (SPJA Case)
                nullrow = True
                for val in row:
                    if val != None:
                        nullrow = False
                        break
                if nullrow == True:
                    continue
                temp = []
                for val in row:
                    temp.append(str(val))
                ins = (tuple(temp))
                cur = reveal_globals.global_conn.cursor()
                cur.execute('INSERT INTO r_h'+str(t1)+' VALUES'+str(ins)+'; ')
                cur.close()

    print("line 89  --   ", time.time() - st)

    
    
    # if len(result) == 2:
    #     cur = reveal_globals.global_conn.cursor()
    #     cur.execute('INSERT INTO r_h'+str(t1)+' VALUES ('+str(result[1][0])+'); ')
    #     cur.close()
    # else:
    # # Filling the table r_h
    #     for i in range(1,len(result)):
    #         cur = reveal_globals.global_conn.cursor()
    #         cur.execute('INSERT INTO r_h'+str(t1)+' VALUES'+str(result[i])+'; ')
    #         cur.close()

    cur  = reveal_globals.global_conn.cursor()
    cur.execute("select sum(hashtext) from (select hashtext(r_e::TEXT) FROM r_e) as T;")
    len1 = cur.fetchone()[0]
    cur.close()
    print("line 108  --   ", time.time() - st)

    cur  = reveal_globals.global_conn.cursor()
    cur.execute("select sum(hashtext) from (select hashtext(r_h::TEXT) FROM r_h) as T;")
    len2 = cur.fetchone()[0]
    cur.close()
    print("line 114  --   ", time.time() - st)

    cur = reveal_globals.global_conn.cursor()
    cur.execute('DROP view r_e;')
    cur.execute('DROP TABLE r_h;')
    cur.close()
    print("line 120  --   ", time.time() - st)
    if(len1 == len2):
        return True
    else:
        return False


