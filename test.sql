--  \i C:/Users/Sneha/Documents/universal_unmasque_folder/unmasque_web_t/UNMASQUE-Web_backup-before-aoa_copy/test.sql

-- Q-5
-- Select  n_name, sum(l_extendedprice) as revenue From  customer, orders, lineitem, supplier, nation, region Where  c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year Group By  n_name Order By  revenue desc Limit  100;
-- Q-17
-- Select  AVG(l_extendedprice) as avgTOTAL From  lineitem, part Where  p_partkey = l_partkey and p_brand = 'Brand#52' and p_container = 'LG CAN';
-- Q-3
-- select l_orderkey, sum(l_discount) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15' group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate, l_orderkey limit 10;
-- Q-1
-- select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_discount) as sum_disc_price, sum(l_tax) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from lineitem where l_shipdate <= date '1998-12-01' - interval '71 days' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus;
-- Q-2
-- select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' order by s_acctbal desc, n_name, s_name, p_partkey limit 100;



-- Q2
-- Select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment From part, supplier, partsupp, nation, region Where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 38 and p_type like '%TIN' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' Order by s_acctbal desc, n_name, s_name Limit 100;

-- Q-5
-- Select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue From customer, orders, lineitem, supplier, nation, region Where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'MIDDLE EAST' and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '1' year Group By n_name Order by revenue desc Limit 100;
-- Q3
-- Select o_orderdate, o_orderpriority, count(*) as order_count From orders Where o_orderdate >= date '1997-07-01' and o_orderdate < date '1997-07-01' + interval '3' month Group By o_orderkey, o_orderdate, o_orderpriority Order by o_orderpriority Limit 10;

-- Q10
-- Select c_name,sum(l_extendedprice) as revenue, c_acctbal, n_name, c_address, c_phone, c_comment From customer, orders, lineitem, nation Where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= date '1994-01-01' and o_orderdate < date '1994-01-01' + interval '3' month and l_returnflag = 'R' and c_nationkey = n_nationkey Group By c_name, c_acctbal, c_phone, n_name, c_address, c_comment Order by revenue desc Limit 20;

-- Q16
Select p_brand, p_type, p_size, count(ps_suppkey) as supplier_cnt From partsupp, part Where p_partkey = ps_partkey and p_brand = 'Brand#45' and p_type Like 'SMALL PLATED%' and p_size >= 4 Group By p_brand, p_type, p_size Order by supplier_cnt desc, p_brand, p_type, p_size;