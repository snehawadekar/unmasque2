#whole from clause taken / copied from aman's code of 13 of tpch_oj

import time
import sys
try:
	import psycopg2
except ImportError:
	pass

sys.path.append('../')

import reveal_globals
import executable


def getconn() :
    #change port 
    conn = psycopg2.connect(
   database=reveal_globals.database_in_use, user='postgres', password='root', host='localhost', port= '5432'
)
    return conn

def establishConnection():
    print("inside------reveal_support.establishConnection")
    reveal_globals.global_db_engine = 'PostgreSQL'
    conn=getconn()
    reveal_globals.global_conn = conn
    print("connected...")
    return True
   
# def check_lenRes():
# 	cur=reveal_globals.global_conn.cursor()
# 	query=reveal_globals.query1
# 	reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
# 	cur.execute(query)
# 	res = cur.fetchall() #fetchone always return a tuple whereas fetchall return list
# 	#print(res)
# 	colnames = [desc[0] for desc in cur.description] 
# 	cur.close()
# 	# result.append(tuple(colnames))
# 	if res is not None:
# 		if len(res)>0:
# 			return True
# 		else:
# 			return False
# 	return False



def getCoreRelations(method = 'rname'):
	#GET ALL TABLE NAMES IN THE DATABASE INSTANCE
	if reveal_globals.global_db_engine != 'Microsoft SQL Server':
		try:
			cur = reveal_globals.global_conn.cursor()
			cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and TABLE_CATALOG='tpch10';")
			# cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and TABLE_CATALOG='" + reveal_globals.global_db_instance + "';")
			res = cur.fetchall()			
			print(res)
			print("from - line 23") #aman
			cur.close()
			for val in res:
				reveal_globals.global_all_relations.append(val[0])
		except Exception as error:
			print("Can not obtain table names. Error: " + str(error))
			return False
	if reveal_globals.global_all_relations == []:
		print("No table in the selected instance. Please select another instance.")
		return False
	print("from - line 33") #aman
	core_relations = []
	if method == 'rename': # 'rename': 
		if 'temp' in reveal_globals.global_all_relations:
			cur = reveal_globals.global_conn.cursor()
			cur.execute('drop table temp;')
			cur.close()
		print("from - line 40") #aman
		for tabname in reveal_globals.global_all_relations:
			try:
				print(tabname)
				cur = reveal_globals.global_conn.cursor()
				#rename current table x to temp
				if reveal_globals.global_db_engine != 'Microsoft SQL Server':
					cur.execute('Alter table ' + tabname + ' rename to temp;')
				cur.close()
				#create an empty table with name x
				#UNCOMMENT THIS FOR EMPTY TABLRE LOGIC
				
				cur = reveal_globals.global_conn.cursor()
				if reveal_globals.global_db_engine != 'Microsoft SQL Server':
					cur.execute('Create table ' + tabname + ' (like temp);')
				cur.close()
				
				# if not(check_lenRes()):
				# 	core_relations.append(tabname)
    
				#check the result
				new_result = executable.getExecOutput()
				reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
				if len(new_result) <= 1:
					core_relations.append(tabname)
				#revert the changes
				#UNCOMMENT THIS FOR EMPTY TABLE LOGIC
				
				cur = reveal_globals.global_conn.cursor()
				cur.execute('drop table ' + tabname + ';')
				cur.close()
				
				cur = reveal_globals.global_conn.cursor()
				if reveal_globals.global_db_engine != 'Microsoft SQL Server':
					cur.execute('Alter table temp rename to ' + tabname + ';')
				cur.close()
				# print("from - line 73") #aman
			except Exception as error:
				print("Error Occurred in table extraction. Error: " + str(error))
				exit(1)
	else: 
		# establishConnection()
		# cur = reveal_globals.global_conn.cursor()
		# cur.execute("set statement_timeout to '2s'")
		# cur.close()
  
		# for extraction of outer joins we need to follows this method
		if 'temp' in reveal_globals.global_all_relations:
			cur = reveal_globals.global_conn.cursor()
			cur.execute('drop table temp;')
			cur.close()
		for tabname in reveal_globals.global_all_relations:
			establishConnection()
			cur = reveal_globals.global_conn.cursor()
			cur.execute("set statement_timeout to '2s'")
			cur.close()
			try:
				cur = reveal_globals.global_conn.cursor()
				if reveal_globals.global_db_engine != 'Microsoft SQL Server':
					print('Alter table ' + tabname + ' rename to temp;')
					cur.execute('Alter table ' + tabname + ' rename to temp;')
				cur.close()
    
				try:
					new_result = executable.getExecOutput() #slow
					reveal_globals.global_no_execCall = reveal_globals.global_no_execCall + 1
					if len(new_result) <= 1:
						core_relations.append(tabname)
				except psycopg2.Error as e:
					# establishConnection()
					if e.pgcode == '42P01':
						core_relations.append(tabname)
					elif e.pgcode != '57014':
						raise
					
				cur = reveal_globals.global_conn.cursor()
				if reveal_globals.global_db_engine != 'Microsoft SQL Server':
					print('Alter table temp rename to ' + tabname + ';')
					cur.execute('Alter table temp rename to ' + tabname + ';')
				cur.close()
			except Exception as error:
				print("Error Occurred in table extraction. Error: " + str(error))
				# exit(1)


		# cur = reveal_globals.global_conn.cursor()
		# cur.execute("set statement_timeout to '0s'")
		# cur.close()
		# establishConnection()

	# print("from - line 113") #aman
	print(core_relations)
	return sorted(core_relations)


#for outer join explain the importance of second from clause extraction method