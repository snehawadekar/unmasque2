import dbcon
import reveal_globals
import result_comparator
import time

hidden_query = [
    
    # Q1
    "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '71 days' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;",
    # Q2
    "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' order by s_acctbal desc, n_name, s_name limit 100;",
    # Q3
    "select l_orderkey, sum(l_discount) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate, l_orderkey limit 10;",
    # Q4
    "Select o_orderdate, o_orderpriority, count(*) as order_count From orders Where o_orderdate >= date '1997-07-01' and o_orderdate < date '1997-07-01' + interval '3' month Group By o_orderdate, o_orderpriority Order By o_orderpriority Limit 10;",
    # Q5
    "Select  n_name, sum(l_extendedprice) as revenue From  customer, orders, lineitem, supplier, nation, region Where  c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year Group By  n_name Order By  revenue desc Limit  100;",
    # Q6
    "Select  l_shipmode, sum(l_extendedprice) as revenue From  lineitem Where  l_shipdate >= date '1994-01-01' and l_shipdate < date '1994-01-01' + interval '1' year and l_quantity < 24 Group By  l_shipmode Limit  100;",
    # Q10
    "Select  c_name, sum(l_extendedprice) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment From  customer, orders, lineitem, nation Where  c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey Group By  c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order By  revenue desc Limit  20;",
    # Q11(n)
    "Select  ps_COMMENT, sum(ps_availqty) as value From  partsupp, supplier, nation Where  ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA' Group By  ps_COMMENT Order By  value desc Limit  100;",
    # Q16
    "Select  p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, part Where  p_partkey = ps_partkey and p_type like 'SMALL PLATED%' and p_size >= 4 Group By  p_brand, p_type, p_size Order By  supplier_cnt desc, p_brand, p_type, p_size;",

    # Q17
    "Select  AVG(l_extendedprice) as avgTOTAL From  lineitem, part Where  p_partkey = l_partkey and p_brand = 'Brand#52' and p_container = 'LG CAN';",
    # Q18
    "Select  p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From  partsupp, part Where  p_partkey = ps_partkey and p_type like 'SMALL PLATED%' and p_size >= 4 Group By  p_brand, p_type, p_size Order By  supplier_cnt desc, p_brand, p_type, p_size;",
    # Q21
     "Select  s_name, count(*) as numwait From  supplier, lineitem l1, orders, nation Where  s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and s_nationkey = n_nationkey and n_name = 'GERMANY' Group By  s_name Order By  numwait desc, s_name Limit  100;"
    # Q23
]


extracted_query = [
    
"Select l_returnflag, l_linestatus, Sum(l_quantity) as sum_qty, Sum(l_extendedprice) as sum_base_price, Sum(l_discount) as sum_disc_price, Sum(l_tax) as sum_charge, Avg(l_quantity) as avg_qty, Avg(l_extendedprice) as avg_price, Avg(l_discount) as avg_disc, count(*) as count_order From lineitem Where l_shipdate <= '1998-09-21' Group By l_returnflag, l_linestatus Order By l_returnflag asc, l_linestatus asc; ",
"Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment From nation, part, partsupp, region, supplier Where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and r_regionkey = n_regionkey and p_size = 38 and p_type LIKE '%TIN' and r_name = 'MIDDLE EAST ' Order By s_acctbal desc, n_name asc , s_name asc Limit 100; ",
"Select l_orderkey, Sum(l_discount) as revenue, o_orderdate, o_shippriority From customer, lineitem, orders Where c_custkey = o_custkey and o_orderkey = l_orderkey and c_mktsegment = 'BUILDING  ' and l_shipdate >= '1995-03-16'  and o_orderdate <= '1995-03-14' Group By l_orderkey, o_orderdate, o_shippriority Order By revenue desc, o_orderdate asc Limit 10; ",
"Select o_orderdate, o_orderpriority, count(*) as order_count From orders Where o_orderdate between date '1997-07-01' and date '1997-09-30' Group By o_orderdate, o_orderpriority Order By o_orderpriority asc, o_orderdate asc Limit 10; ",
"Select n_name, Sum(l_extendedprice) as revenue From customer, lineitem, nation, orders, region, supplier Where s_suppkey = l_suppkey and s_nationkey = n_nationkey and s_nationkey = c_nationkey and c_custkey = o_custkey and o_orderkey = l_orderkey and r_regionkey = n_regionkey and o_orderdate between date '1994-01-01' and date '1994-12-31' and r_name = 'MIDDLE EAST              ' Group By n_name Order By revenue desc, n_name asc Limit 100; ",
"Select l_shipmode, Sum(l_extendedprice) as revenue From lineitem Where l_quantity <= 23.99 and l_shipdate between date '1994-01-01' and date '1994-12-31' Group By l_shipmode Order By l_shipmode asc Limit 100;",
"Select c_name, Sum(l_extendedprice) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment From customer, lineitem, nation, orders Where n_nationkey = c_nationkey and c_custkey = o_custkey and o_orderkey = l_orderkey and l_returnflag = 'R' and o_orderdate between date '1994-01-01' and date '1994-03-31' Group By c_acctbal, c_phone, c_comment, c_name, c_address, n_name Order By revenue desc, c_name asc, c_acctbal asc, c_phone asc, n_name asc, c_address asc, c_comment asc Limit 20;",
"Select ps_comment, Sum(ps_availqty) as value From nation, partsupp, supplier Where s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_name = 'ARGENTINA                ' Group By ps_comment Order By value desc, ps_comment asc Limit 100;",
"Select p_brand, p_type, p_size, count(*) as supplier_cnt From part, partsupp Where p_partkey = ps_partkey and p_size >= 4 and p_type LIKE 'SMALL PLATED%' Group By p_size, p_brand, p_type Order By supplier_cnt desc, p_brand asc, p_type asc, p_size asc;",
"Select Avg(l_extendedprice) as avgtotal From lineitem, part Where p_partkey = l_partkey and p_brand = 'Brand#52  ' and p_container = 'LG CAN    ' Limit 1; ",
"Select p_brand, p_type, p_size, count(*) as supplier_cnt From part, partsupp Where p_partkey = ps_partkey and p_size >= 4 and p_type LIKE 'SMALL PLATED%' Group By p_size, p_brand, p_type Order By supplier_cnt desc, p_brand asc, p_type asc, p_size asc; ",
"Select s_name, count(*) as numwait From lineitem, nation, orders, supplier Where s_suppkey = l_suppkey and s_nationkey = n_nationkey and o_orderkey = l_orderkey and n_name = 'GERMANY                  ' and o_orderstatus = 'F' Group By s_name Order By numwait desc, s_name asc Limit 100; ",
    
]

dbcon.establishConnection()

# for i in range(len(hidden_query)):
#     # print(hidden_query[i])
#     st = time.time()
#     reveal_globals.query1 = hidden_query[i]
#     # ans = result_comparator.match_comparison_based(extracted_query[i])
#     ans = result_comparator.match(extracted_query[i])
#     print("total time   :", time.time() - st)
#     print (ans, i)


st = time.time()
reveal_globals.query1 = " SELECT  o_shippriority ,count(o_orderpriority) as low_line_count FROM lineitem , orders WHERE l_orderkey = o_orderkey AND o_totalprice < 50000 and  l_shipmode IN ('MAIL', 'SHIP', 'TRUCK', 'AIR', 'FOB', 'RAIL') AND l_commitdate <= l_receiptdate AND l_shipdate <= l_commitdate AND ( l_receiptdate >= '1994-01-01' OR l_quantity < 30 ) AND l_receiptdate < '1995-01-01' GROUP BY  o_shippriority LIMIT  10; "
# ans = result_comparator.match_comparison_based(extracted_query[i])
ans = result_comparator.match(" Select o_shippriority, count(*) as low_line_count  From lineitem, orders Where l_shipdate <= l_commitdate and l_commitdate >= l_shipdate and l_commitdate <= l_receiptdate and l_receiptdate >= l_commitdate and l_receiptdate <= '1994-12-31' and l_commitdate <= '1994-12-31' and l_shipdate <= '1994-12-31' and l_orderkey = o_orderkey and l_quantity <= 29.99 and (l_shipdate <= '1993-07-23' or l_receiptdate between date '1993-08-02' and date '1994-12-31' or l_receiptdate between date '1993-07-29' and date '1994-12-31') and (l_commitdate between date '1993-07-12' and date '1993-08-05' or l_receiptdate between date '1993-08-12' and date '1994-12-31' or l_receiptdate between date '1993-07-11' and date '1994-12-31') and (l_receiptdate between date '1993-07-23' and date '1994-12-31' or l_receiptdate between date '1993-07-14' and date '1994-12-31') and (l_shipmode = 'FOB       ' or l_shipmode = 'AIR       ' or l_shipmode = 'SHIP      ' or l_shipmode = 'MAIL      ' or l_shipmode = 'RAIL      ' or l_shipmode = 'TRUCK     ') and o_totalprice <= 49999.99 Group By o_shippriority Order By o_shippriority asc Limit 10; ")
print("total time   :", time.time() - st)
print (ans)
