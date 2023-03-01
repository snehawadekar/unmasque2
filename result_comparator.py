import reveal_globals
import sys

# HAsh based result comparator
def match(Q_E, res):
        
    cur  = reveal_globals.global_conn.cursor()
    cur.execute("create view r_e as "+ Q_E)
    cur.close()
    
    cur  = reveal_globals.global_conn.cursor()
    cur.execute("Select count(*) from r_e")
    res1 = cur.fetchone()[0]
    cur.close()
    

    
    if(res1 != (len(res) - 1)):
        return False

    result = res
    
    cur = reveal_globals.global_conn.cursor()
    cur.execute('Create unlogged table r_h (like r_e);')
    cur.close()
 
    # Header of r_h
    t = result[0]
    t1 = '(' + t[0]
    for i in range(1,len(t)):
        t1 += ', ' + t[i]
    t1 += ')'

    # Filling the table r_h
    for i in range(1,len(result)):
        cur = reveal_globals.global_conn.cursor()
        cur.execute('INSERT INTO r_h'+str(t1)+' VALUES'+str(result[i])+'; ')
        cur.close()

    cur  = reveal_globals.global_conn.cursor()
    cur.execute("select sum(hashtext) from (select hashtext(r_e::TEXT) FROM r_e) as T;")
    len1 = cur.fetchone()[0]
    cur.close()

    cur  = reveal_globals.global_conn.cursor()
    cur.execute("select sum(hashtext) from (select hashtext(r_h::TEXT) FROM r_h) as T;")
    len2 = cur.fetchone()[0]
    cur.close()

    cur = reveal_globals.global_conn.cursor()
    cur.execute('DROP view r_e;')
    cur.execute('DROP TABLE r_h;')
    cur.close()
    if(len1 == len2):
        return True
    else:
        return False


