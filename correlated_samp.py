import executable 
import reveal_globals
import time
import psycopg2
import psycopg2.extras

pk = {'region': 'r_regionkey', 'customer': 'c_custkey', 'orders': 'o_orderkey', 'nation': 'n_nationkey', 'part': 'p_partkey', 'supplier': 's_suppkey', 'lineitem' : 'l_linenumber', 'partsupp' : 'ps_partkey'}


tables = ['customer', 'lineitem', 'orders', 'nation']
cs = {}
tf = 0
ind = 2

def getCoreSizes_cs(core_relations):
	core_sizes = {}
	for table in core_relations:
		try:
			cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
			cur.execute("SELECT reltuples AS estimate FROM pg_class where relname = '" + table + "';")
			res = cur.fetchone()
			cur.close()
			cnt = int(float(str(res[0])))
			if cnt == 0:
				cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
				cur.execute('select count(*) from ' + table + ';')
				res = cur.fetchone()
				cnt = int(str(res[0]))
			core_sizes[table] = cnt
		except Exception as error:
			print("Error in getting table Sizes. Error: " + str(error))
	return core_sizes

def correlated_sampling_start():
	itr=5
	cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	for table in reveal_globals.global_all_relations:
		cur.execute("alter table " + table + " rename to " + table + "_tmp;")
	cur.close()
	#restore original tables somewhere
	start_time=time.time()
	while itr>0:
		if correlated_sampling()== False:
			print('sampling failed in iteraation', itr)
			itr = itr-1
		else:
			cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
			for table in reveal_globals.global_all_relations:
				cur.execute("drop table " + table + "_tmp;")

			cur.close()
			reveal_globals.cs_time = time.time() - start_time

			print("CS PASSED")
   
			return

	print("correlated samplin failed totally starting with halving based minimization")
	#
	cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	for table in reveal_globals.global_all_relations:
		cur.execute("alter table " + table + "_tmp rename to " + table + " ;")
	cur.close()
	# cs sampling time
	reveal_globals.cs_time = time.time() - start_time
	return

			
   
        

    
def correlated_sampling():
    # view based correlated sampling
	print("Starting correlated sampling ")
	p = 0
	res = 0
	n = 10000
	flag = 0
	cnt = 0
	tabname = 'lineitem_tmp'
	cur = reveal_globals.global_conn.cursor()
	cur.execute('select count(*) from ' + tabname + ';')
	res = cur.fetchone()
	cnt = int(str(res[0]))
	pt = (n/cnt) * 100
	if pt > 100:
		pt = 100
	p = pt
	cur.close()
	print(n,p)
	


	#calculate initial sampling percentage for lineitem
	cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	for table in reveal_globals.global_all_relations:
		cur.execute("create unlogged table " + table + " (like " + table + "_tmp);")
	cur.close()

	#genereate correlated sample synopsis
	cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cur.execute("insert into lineitem select * from lineitem_tmp tablesample system(" + str(p) + ");")
	cur.execute("insert into orders select * from orders_tmp where o_orderkey in (select distinct(l_orderkey) from lineitem) on conflict do nothing;")
	cur.execute("insert into customer select * from customer_tmp where c_custkey in (select distinct(o_custkey) from orders) on conflict do nothing;")
	# cur.execute("insert into nation select * from nation_tmp where n_nationkey in (select distinct(c_nationkey) from customer) on conflict do nothing;")
	cur.execute("insert into region select * from region_tmp where r_regionkey in (select distinct(n_regionkey) from nation);")
	cur.execute("insert into partsupp select * from partsupp_tmp where exists(select * from lineitem where ps_suppkey = l_suppkey and ps_partkey = l_partkey);")
	cur.execute("insert into part select * from part_tmp where p_partkey in (select distinct(ps_partkey) from partsupp);")
	cur.execute("insert into supplier select * from supplier_tmp where s_suppkey in (select distinct(ps_suppkey) from partsupp);")
	cur.execute("insert into nation select * from nation_tmp where n_nationkey in (select distinct(s_nationkey) from supplier) on conflict do nothing;")
	cur.execute("insert into region select * from region_tmp where r_regionkey in (select distinct(n_regionkey) from nation) on conflict do nothing;")
	cur.close()

  
	# cs={}
	# for table in reveal_globals.global_all_relations:
	# 	cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
	# 	cur.execute('select count(*) from ' + table + ';')
	# 	t = cur.fetchone()
	# 	cs[table] = int(str(t[0]))
	# 	cur.close()
	# print(cs)
	new_result= executable.getExecOutput()
	if len(new_result)<=1:
		print('sampling failed in iteraation')
		cur = reveal_globals.global_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
		for table in reveal_globals.global_all_relations:
				cur.execute("drop table " + table + ";")
		cur.close()
		return False
	else:
		# drop original tables
		# convert views to tables 
		return True

	


