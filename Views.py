
#working correct last update :dec 10,2022

import os
import sys
import csv
import copy
import math
import executable
sys.path.append('../') 
import reveal_globals
import psycopg2


#Views - Mukul

#RowId is the additional created column in tabname and index is created on it
#All the views are created on tabname2 table
#Partition contains the starting rowid and size of given table

def Views_Halving(tabname, partition, mid):
    #Drop View tabname_U
    cur = reveal_globals.global_conn.cursor()
    cur.execute('Drop View if exists ' + tabname + '_U ;')
    cur.close()
    #Drop View tabname_L
    cur = reveal_globals.global_conn.cursor()
    cur.execute('Drop View if exists ' + tabname + '_L ;')
    cur.close()
    #Create view having upper half
    cur = reveal_globals.global_conn.cursor()
    cur.execute('Create View ' + tabname + '_U as Select * from '+ tabname + '2 where rowid >= ' +  str(partition[0]) + ' and rowid <=' + str(partition[0]+mid) + ';')
       
    cur.close()
    #Create view having lower half
    cur = reveal_globals.global_conn.cursor()
    cur.execute('Create View ' + tabname + '_L as Select * from '+ tabname + '2 where rowid >= ' +  str(partition[0] + mid) + ' and rowid <= ' + str(partition[1]) + ';')
    cur.close()

