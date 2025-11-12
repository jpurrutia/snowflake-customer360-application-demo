
-- 11.0.0  Working with Clustering
--         In this lab you will learn and practice the following:
--         How to identify
--         - Clustered and non clustered tables
--         - Determine the cluster key (if defined)
--         - See the size and number of rows for tables
--         - Determine the status of clustering for a table (active or
--         suspended)
--         Understand how to perform the following actions on a table
--         - Define a cluster key
--         - Suspend Clustering
--         - Resume Clustering
--         - Review detailed clustering information for a given table
--         - Show if a column would be a good or bad candidate for clustering
--         - Understand how to perform an initial and ongoing data clustering
--         - Demonstrate the performance impact using clustered and non
--         clustered tables
--         This lab provides concepts as well as detailed instructions for
--         implementing clustering. The data used within will be taken from
--         database share SNOWFLAKE_SAMPLE_DATA.

-- 11.1.0  Identify Clustered / Non Clustered Tables
--         This section shows you how to obtain the following details about
--         tables.
--         Clustered or not clustered
--         Clustering /Active or Suspended
--         Table Size and Number Of Rows

-- 11.1.1  Set your context.

USE ROLE arch_role;
CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh;
USE WAREHOUSE TAPIR_arch_wh;
CREATE DATABASE IF NOT EXISTS TAPIR_arch_db;
CREATE SCHEMA IF NOT EXISTS TAPIR_arch_db.cluster_test;
USE SCHEMA TAPIR_arch_db.cluster_test;



-- 11.1.2  Show Largest tables with cluster details and size details.

SHOW TABLES IN account;
SELECT "database_name"        AS DATABASE_NAME
      ,"schema_name"          AS SCHEMA_NAME
      ,"name"                 AS TABLE_NAME
      ,"cluster_by"           AS CLUSTER_BY
      ,"automatic_clustering" AS AUTO_CLUSTER
      ,ROUND("bytes"::NUMBER(38,0)/POW(1024,3)) AS SIZE_Gb
      ,"rows"                 AS ROW_COUNT
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY SIZE_GB DESC;

--         The result should look something like:
--         Note: The SQL is in two sections:
--         Show Tables in account This is used to obtain details about tables in
--         permanent databases in the account and tables that belong to database
--         shares. SNOWFLAKE.ACCOUNT_USAGE.TABLES does not show tables that part
--         of a share SNOWFLAKE_SAMPLE_DATA.INFORMATION_SCHEMA.TABLES will show
--         only the tables in the share SNOWFLAKE_SAMPLE_DATA This by using the
--         SHOW command, you get everything you have access to.
--         The query is ordered by size to show the largest tables first
--         The second part, queries from TABLE(RESULT_SCAN(LAST_QUERY_ID())) The
--         column names are case sensitive, which is why you see the column
--         names in double quotes, the alias given can not case sensitive and
--         can used further down in the query. E.g. The ORDER BY
--         The column cluster_by shows what the cluster key is. The absence of a
--         value here indicates the table is not clustered
--         The column auto_cluster can have two values
--         - ON Auto Clustering is active
--         - OFF Auto Clustering is suspended.

-- 11.1.3  Examine the detail cluster information.
--         Looking at the largest table, we will examine the detailed clustering
--         information. The intention here is to become familiar with looking at
--         the clustering details.

-- 11.1.4  Examine the Clustering Detail.
--         To perform this exercise, we use the function
--         system$clustering_information

SELECT
system$clustering_information
('SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.STORE_SALES','(ss_sold_date_sk,ss_item_sk)');

--         The output of this query should look like this:
--         Note: This output shows good clustering due to
--         - Average Depth is <= 10, the output shows 2.7
--         - The histogram shows all micro partitions have a depth of 1,2 or 3

-- 11.1.5  We can also use this function to see how well the table is currently
--         clustered by the given column(s), regardless of the cluster key.

SELECT
system$clustering_information
('SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.STORE_SALES','(SS_ADDR_SK)');

--         The output of this query should look like this:
--         Note: This output shows bad clustering due to
--         - Average Depth is > 10, the output shows 721507. This is equal to
--         the total number of partitions meaning a where clause against this
--         column would always result in a full table scan.
--         - The histogram shows all micro partitions have a depth of 1,048,576.
--         This number is rather strange and does not bear a huge meaning. This
--         is the point at which Snowflake determined it was not worth looking
--         any deeper. Given that this is greater than the total number of
--         partitions, there is no point in looking any further.
--         - This table is not currently clustered well by the address column.

-- 11.2.0  Demonstrate How To Create, Enable and Disable Clustering
--         This exercise shows the following
--         - Creating a cluster key
--         - Disabling Auto-Clustering
--         - Enabling Auto-Clustering

-- 11.2.1  To perform these tasks, let’s take a copy of the table
--         SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS.
--         This should take approximately 10-15 seconds

-- Take Copy of this table
CREATE OR REPLACE TABLE orders_v1 AS SELECT * FROM snowflake_sample_data.tpch_sf10.orders;

--         Lets take a look at the table using SHOW TABLE

-- Show The table
show tables like 'ORDERS_V1';

--         The output of this should look like this:
--         Note: The result shows the absence of a cluster key as the column
--         cluster_by is blank, thus automatic_clustering is also OFF

-- 11.2.2  Create the cluster key.

-- Create a Cluster Key
ALTER TABLE orders_v1 CLUSTER BY (o_orderdate);


-- 11.2.3  Re-Run the show table to see any differences.

-- Show it
SHOW TABLES LIKE 'ORDERS_V1';

--         The output should look like this:
--         Note: The result shows a value LINEAR(o_orderdate) in the cluster_by,
--         this now tells us the table is clustered. The column
--         automatic_clustering shows a value of ON, this tells us the auto
--         clustering is enabled / active

-- 11.2.4  Lets now suspend clustering on this table.

-- Switch clustering off for this table
ALTER TABLE orders_v1 SUSPEND RECLUSTER;


-- 11.2.5  Re-Run the show table to see any differences.

-- Show it
SHOW TABLES LIKE 'ORDERS_V1';

--         The output should look like this:
--         Note: The result shows a value LINEAR(o_order_date) in the
--         cluster_by, this now tells us the table is clustered.
--         The column automatic_clustering shows a value of OFF, this tells us
--         the auto clustering suspended.

-- 11.2.6  Now, re-enable the clustering.

-- Resume clustering
ALTER TABLE orders_v1 RESUME RECLUSTER;


-- 11.2.7  Re-Run the show table to see any differences.

-- Show it
SHOW TABLES LIKE 'ORDERS_V1';

--         The output should look like this:

-- 11.2.8  As clustering incurs cost and we no longer need this table, lets
--         switch off the auto clustering.

-- Switch clustering off for this table
ALTER TABLE orders_v1 SUSPEND RECLUSTER;


-- 11.3.0  Demonstrate the performance impact of clustering.
--         In this exercise, there are two objectives
--         - Demonstrate that when clustering is first enabled against a table,
--         nothing immediatley happens
--         - Demonstrate the effect clustering has on micro partition pruning.
--         - Demonstrate that is faster to load re-load the table in a ordered
--         manner, then enable clustering
--         To conduct this exercise we will create a copy of the table
--         SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.ORDERS but have the o_orderdate
--         loaded in a random order. Using this table, we will create two
--         clones.
--         The first clone will be used to enable clustering on an unordered
--         table
--         The second clone will be used to re-order the data first and then
--         enable clustering

-- 11.3.1  Let’s create the tables.

-- Create a fresh copy the the orders table and randomize the order date by initially ordering by the orderkey.
CREATE OR REPLACE TABLE orders_v2 AS SELECT * FROM snowflake_sample_data.tpch_sf10.orders ORDER BY o_orderkey;

-- Clone this table as is
CREATE  OR REPLACE TABLE orders_v2_cluster CLONE orders_v2;

-- Clone the table and re-order it by o_orderdate.
CREATE  OR REPLACE TABLE orders_v2_sorted CLONE orders_v2;
INSERT OVERWRITE INTO orders_v2_sorted SELECT * FROM orders_v2_sorted ORDER BY o_orderdate;


-- 11.3.2  Let’s check these tables exist.

SHOW TABLES LIKE 'ORDERS_V2%' IN SCHEMA;

--         The output of the show command should look like this:
--         Note: The result shows that none of the tables created are clustered
--         due to the column cluster_by shows no values.

-- 11.3.3  Lets look at the clustering information on the un-ordered table
--         (orders_v2_cluster).

SELECT system$clustering_information('orders_v2_cluster','(o_orderdate)');

--         The output from above should look like this:
--         Note: There are a total of 24 micro partitions and we have an average
--         depth of 24. This suggests a full scan when using o_orderdate a where
--         clause against this table.

-- 11.3.4  Now, add the cluster key to this table and examine the result.

-- Now add a cluster key to the unclustered table
ALTER TABLE orders_v2_cluster CLUSTER BY (o_orderdate);


-- See details;
SELECT system$clustering_information('orders_v2_cluster','(o_orderdate)');

--         The output of the clustering information should look like this:
--         Note: As can be seen, not much has changed immediately. We will
--         complete the remainder of the exercise and come back to review the
--         clustering information on this table.

-- 11.3.5  Let’s perform the same action against the sorted table.

-- Now add a cluster key to the unclustered table
ALTER TABLE orders_v2_sorted CLUSTER BY (o_orderdate);


-- See details;
SELECT system$clustering_information('orders_v2_sorted','(o_orderdate)');

--         The output of the clustering information should look like this:
--         Note: Clustering is a background process that occurs. You need to
--         wait before getting the desired results. The average cluster depth is
--         2 (Good), and the histogram reflects this too.

-- 11.3.6  Test a query against the sorted table.

-- Run query against the sorted table
SELECT *
FROM orders_v2_sorted
WHERE o_orderdate = '1998-05-28'::DATE;


-- 11.3.7  And take a look at the query profile.
--         It can be seen that partition pruning has taken place, 1 partition
--         scanned out of 24.

-- 11.3.8  Now, let’s examine the cluster information on the unsorted table,
--         remember clustering is enabled for this table.

-- See details;
SELECT system$clustering_information('orders_v2_cluster','(o_orderdate)');

--         The output of the clustering information should look like this:
--         Note: Clustering is a background process. You may need to wait to see
--         proper results. The image above now shows a significant difference.
--         The cluster depth has reduced to 3 and the histogram shows that too.
--         The result of your query may differ from above as its all down to
--         timing. The intent here, is to show that re-ordering of the data has
--         occurred in the background.

-- 11.3.9  Let’s compare the query profiles of the unsorted table, the sorted
--         table and the table that was unsorted but clustering added.
--         Note: The unsorted table shows a full scan - no partition pruning The
--         sorted table shows that only one partition was scanned The clustered
--         table show that only 2 out of 24 partitions were scanned. As time
--         continues, this value will improve.
--         This example showed clustering over a small table, in reality,
--         clustering isn’t needed to smaller tables. A general rule is that you
--         clustering becomes are higher probability as the table approaches 1TB
--         in size.
--         For best results, sort the table first, then apply the clustering
--         key.

-- 11.3.10 Tidy Up the lab.
--         Now lets remove the tables we created such that we’re not using space
--         or compute unnecessarily.

-- Drop tables
DROP TABLE orders_v2_sorted;
DROP TABLE orders_v2_cluster;
DROP TABLE orders_v2;
DROP TABLE orders_v1;
USE SCHEMA TAPIR_arch_db.public;
DROP SCHEMA TAPIR_arch_db.cluster_test;


-- 11.4.0  Key Takeaways
--         In this lab you have learned
--         - You can also use the system$clustering_information function to see
--         how well the table is currently clustered by the given column(s),
--         regardless of the cluster key.
--         - The output of a SHOW TABLE command can be used to determine if auto
--         clustering is enabled / active.
