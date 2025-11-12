
-- 12.0.0  Search Optimization
--         By the end of this lab, you will be able to:
--         - Add Search Optimization to a set of columns in a table.
--         - Check that Search Optimization is configured and ready.
--         - Run performance testing to verify it works.

-- 12.1.0  Setup Lab Environment
--         In this exercise you will setup a database, schema, and a table that
--         will be used for all the exercises in this lab.

-- 12.1.1  Open a new worksheet or Create Worksheet from SQL File and set your
--         context and create schema:

USE ROLE arch_role;
CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh;
USE WAREHOUSE TAPIR_arch_wh;

CREATE DATABASE IF NOT EXISTS TAPIR_arch_db;
CREATE SCHEMA IF NOT EXISTS TAPIR_arch_db.search_opt;
USE SCHEMA TAPIR_arch_db.search_opt;


-- 12.1.2  Alter the warehouse to a 2X-Large to perform the copy.

ALTER WAREHOUSE TAPIR_arch_wh SET WAREHOUSE_SIZE='2X-LARGE'
WAIT_FOR_COMPLETION = TRUE;


-- 12.1.3  Create a large table, this will take approx. 45 seconds;

CREATE OR REPLACE TABLE  myOrders AS
SELECT *
FROM snowflake_sample_data.tpch_sf1000.orders
ORDER BY o_orderdate;


-- 12.2.0  Run query without search optimization

-- 12.2.1  Alter the warehouse to a Xsmall for our performance testing.

ALTER WAREHOUSE TAPIR_arch_wh SET WAREHOUSE_SIZE='XSMALL'
WAIT_FOR_COMPLETION = TRUE;


-- 12.2.2  Turn off cache results for testing.

ALTER SESSION SET use_cached_result = false;


-- 12.2.3  Use show table to examine the status.
--         Example table - note the column search optimization is set to OFF

SHOW TABLES LIKE 'myOrders';


-- 12.2.4  Execute the following query without search optimization.
--         Should take about 20 secs

-- Make sure warehouse is running
ALTER WAREHOUSE TAPIR_arch_wh RESUME IF SUSPENDED;

-- Then run this query and note how long it takes to complete
SELECT *
FROM myorders
WHERE o_custkey = 97647196;


-- 12.2.5  Examine the query profile.
--         You should see that most of the 2662 micropartions are used.

-- 12.3.0  Add Search Optimization to a column

-- 12.3.1  Optimize table for equality search on o_custkey.

ALTER TABLE myOrders ADD SEARCH OPTIMIZATION ON EQUALITY(o_custkey);


-- 12.4.0  Check that Search Optimization is configured and ready then re-run
--         query

-- 12.4.1  Check the status of the table.
--         Once the search_optimization status = ON AND
--         search_optimization_progress = 100, then its ready for to re-run
--         query query.
--         It may take 4 or 5 minutes for the search optimization to complete,
--         so be patient.

SHOW TABLES LIKE 'myOrders';
SELECT "search_optimization_progress" FROM table(result_scan(last_query_id()));

--         You will probably need to rerun the SHOW TABLES and SELECT commands
--         multiple times until progress gets to 100.

-- 12.4.2  Suspend then resume warehouse to clear cache.

ALTER WAREHOUSE TAPIR_arch_wh SUSPEND;

ALTER WAREHOUSE TAPIR_arch_wh RESUME;


-- 12.4.3  With search optimization ON re-run the query you did before.

SELECT *
FROM myorders
WHERE o_custkey = 97647196;


-- 12.4.4  Examine the query profile now.
--         You should see 29 micro partitions used and the query should take < 3
--         or 4 seconds.

-- 12.4.5  Tidy up by removing the schema and table within it.

USE SCHEMA TAPIR_arch_db.public;
DROP SCHEMA TAPIR_arch_db.search_opt CASCADE;
ALTER WAREHOUSE TAPIR_arch_wh SUSPEND;


-- 12.5.0  Key Takeaways
--         In this lab you have learned
--         - To check the status of the table where Search Optimization has been
--         configured use the SHOW TABLE command. Once the search_optimization
--         status = ON AND search_optimization_progress = 100, then it is
--         complete and ready to use.
--         - Always re-run your test queries to make sure you are getting the
--         performance gains expected. There is a storage cost and processing
--         cost when using Search Optimization so be selective in your approach.

