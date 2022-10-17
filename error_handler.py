import sys
import psycopg2
sys.path.append('../')
import reveal_globals
def restore_database_instance():
	if reveal_globals.global_restore_flag != True:
		print('Restoring database instance to initial state.')
	else:
		return
	for tabname in reveal_globals.global_core_relations:
		try:
			cur = reveal_globals.global_conn.cursor()
			cur.execute('alter table ' + tabname + '1 rename to ' + tabname + '2;')
			#The above command will inherently check if tabname1 exists
			cur.execute('drop table ' + tabname + ';')
			cur.execute('alter table ' + tabname + '2 rename to ' + tabname + ';')
			cur.close()
		except:
			pass
		try:
			cur = reveal_globals.global_conn.cursor()
			cur.execute('drop table ' + tabname + '4;')
			cur.close()
		except:
			pass
	#hardcoding for demo
	
	if reveal_globals.global_conn is not None:
		reveal_globals.global_conn.close()
		reveal_globals.global_conn = None
	reveal_globals.global_restore_flag = True