
-- 13.0.0  Materialized View Use Cases
--         In this lab you will learn and practice the following:
--         - Clustering a Table using a Timestamp Column Type
--         - Clustering a Table to Improve Performance
--         - Exploring Automatic Transparent Rewrite on Materialized Views
--         - Putting Materialized Views on External Tables
--         In this lab you will be working with Materialized Views. A
--         materialized view is a pre-computed data set derived from a query
--         specification (the SELECT in the view definition) and stored for
--         later use. Because the data is pre-computed, querying a materialized
--         view is faster than executing a query against the base table of the
--         view.

-- 13.1.0  Cluster a Table Using a Timestamp Column

-- 13.1.1  Open a new worksheet or Create Worksheet from SQL File and set your
--         context

USE ROLE arch_role;
CREATE DATABASE IF NOT EXISTS TAPIR_arch_db;
USE DATABASE TAPIR_arch_db;
USE SCHEMA public;
CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh;
USE WAREHOUSE TAPIR_arch_wh;

ALTER WAREHOUSE TAPIR_arch_wh set warehouse_size = xsmall;
ALTER SESSION SET USE_CACHED_RESULT=TRUE;



-- 13.1.2  Create a table using cloning


CREATE OR REPLACE TABLE weblog CLONE training_db.traininglab.weblog;


-- 13.1.3  Check the clustering quality of the CREATE_MS and METRIC9 columns

SELECT SYSTEM$CLUSTERING_INFORMATION( 'weblog' , '(create_ms)');

SELECT SYSTEM$CLUSTERING_INFORMATION( 'weblog' , '(metric9)');

--         Compare the clustering information results. Which column is more
--         effectively clustered?

-- 13.1.4  Run a query with a search filter using the column CREATE_MS

SELECT COUNT(*) CNT
     , AVG(time_on_load_ms) AVG_TIME_ON_LOAD
FROM WEBLOG
WHERE create_ms BETWEEN 1000000000 AND 1000001000;


-- 13.1.5  View the query profile to check micro-partition pruning
--         Click on the TableScan[3] node in the diagram and examine the
--         partition statistics.
--         In this case, the micro-partition pruning is very good.

-- 13.1.6  Check the clustering quality of the column PAGE_ID
--         Based on the column name - would you expect it to be well-clustered,
--         or poorly clustered?

SELECT SYSTEM$CLUSTERING_INFORMATION( 'weblog' , '(page_id)' );

--         Were you right?

-- 13.1.7  Run a query that filters in the PAGE_ID
--         Since PAGE_ID is not well-clustered, you would expect the micro-
--         partition pruning to be low (i.e., we’ll likely need to scan all, or
--         nearly all, of the micro-partitions). This query takes about 1:15
--         min.

SELECT COUNT(*) CNT
     , AVG(time_on_load_ms) AVG_TIME_ON_LOAD
FROM WEBLOG
WHERE page_id=100000;


-- 13.1.8  Check the micro-partition pruning in the query profile
--         Click on the TableScan[3] node in the diagram and examine the
--         partition statistics.
--         Note that, as expected, the micro-partition pruning is very poor.
--         Record the execution time, and the micro-partition pruning.

-- 13.2.0  Cluster a Table to Improve Query Performance
--         You would like both queries - the one filtered by PAGE_ID and the one
--         filtered by CREATE_MS - to run fast. But running both queries with
--         equally good performance requires using a second copy of the data
--         that’s organized differently. You can do this easily with
--         materialized views.

-- 13.2.1  Create a materialized view clustered by PAGE_ID
--         Creating the materialized view with a clustering key causes Snowflake
--         to reorganize the data during the initial creation of the
--         materialized view. Here you will increase the virtual warehouse size
--         so the re-clustering will go faster - but the operation will still
--         take up to 3 minutes. This would be a good time to stretch your legs
--         or refill your coffee.

-- 13.2.2  Set warehouse size

ALTER WAREHOUSE TAPIR_arch_wh SET
  WAREHOUSE_SIZE = XXLARGE
  WAIT_FOR_COMPLETION = TRUE;

CREATE OR REPLACE MATERIALIZED VIEW mv_time_on_load (
  create_ms,
  page_id,
  time_on_load_ms
)
CLUSTER BY (page_id)
AS
SELECT
  create_ms,
  page_id,
  time_on_load_ms
FROM weblog;


-- 13.2.3  Check clustering efficiency on the PAGE_ID column of the materialized
--         view

SELECT SYSTEM$CLUSTERING_INFORMATION ( 'mv_time_on_load' , '(page_id)' );

--         After the clustering, the average_depth should be around 2 or 3. This
--         is quite an improvement.

-- 13.2.4  Run the query filtered on PAGE_ID against the materialized view
--         For a proper performance comparison, set the warehouse size back to
--         what it was the first time the query ran.

ALTER WAREHOUSE TAPIR_arch_wh SET
  WAREHOUSE_SIZE = xsmall
  WAIT_FOR_COMPLETION = TRUE;

SELECT COUNT(*),
       AVG(time_on_load_ms) AVG_TIME_ON_LOAD
FROM mv_time_on_load
WHERE page_id=100000;

--         This example illustrates a substantial improvement in terms of query
--         performance.

-- 13.2.5  Check micro-partition pruning in the query profile
--         With the materialized view, only one micro-partition was scanned.
--         There was a significant increase in performance as a result.

-- 13.2.6  SHOW materialized views on the WEBLOG table

SHOW MATERIALIZED VIEWS ON weblog;


-- 13.3.0  Explore Automatic Transparent Rewrite on Materialized Views
--         The Snowflake query optimizer can exploit materialized views to
--         automatically rewrite/reroute queries made against the source table,
--         to the materialized view.

-- 13.3.1  Use EXPLAIN to see if a command will use a source table or a
--         materialized view
--         Use explain to check if a query against the original source table
--         will use a materialized view for query performance

EXPLAIN
  SELECT COUNT(*) CNT,
         AVG(time_on_load_ms) AVG_TIME_ON_LOAD
FROM weblog
WHERE page_id=100000;

--         Note that even though the query was against the WEBLOG table, the
--         EXPLAIN plan shows that the materialized view will be scanned.

-- 13.3.2  Run the query

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT COUNT(*) CNT,
       AVG(time_on_load_ms) AVG_TIME_ON_LOAD
FROM weblog
WHERE page_id=100000;


-- 13.3.3  Check the query profile
--         Even though the query was against the WEBLOG table, the materialized
--         view was scanned instead.

-- 13.4.0  Materialized Views on External Tables

-- 13.4.1  Create a file format for an external table

CREATE OR REPLACE FILE FORMAT txt_fixed_width
  TYPE = CSV
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = NONE
  RECORD_DELIMITER = '\\n'
  SKIP_HEADER = 0
  TRIM_SPACE = FALSE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  NULL_IF = ('\\N');


-- 13.4.2  Create an external table with partitions based on the filename

CREATE OR REPLACE EXTERNAL TABLE finwire
(
  year                VARCHAR(4)   AS SUBSTR(METADATA$FILENAME, 16, 4),
  quarter             VARCHAR(1)   AS SUBSTR(METADATA$FILENAME, 21, 1),
  thestring           VARCHAR(90)  AS  SUBSTR(METADATA$FILENAME, 1, 50),
  pts                 VARCHAR(15)  AS SUBSTR($1, 8, 15),
  rec_type            VARCHAR(3)   AS SUBSTR($1, 23, 3),
  company_name        VARCHAR(60)  AS SUBSTR($1, 26, 60),
  cik                 VARCHAR(10)  AS SUBSTR($1, 86, 10),
  status              VARCHAR(4)   AS
    IFF(SUBSTR($1, 23, 3) = 'CMP', SUBSTR($1, 96, 4),SUBSTR($1, 47, 4)),
  industry_id         VARCHAR(2)   AS SUBSTR($1, 100, 2),
  sp_rating           VARCHAR(4)   AS SUBSTR($1, 102, 4),
  founding_date       VARCHAR(8)   AS SUBSTR($1, 106, 8),
  addr_line1          VARCHAR(80)  AS SUBSTR($1, 114, 80),
  addr_line2          VARCHAR(80)  AS SUBSTR($1, 194, 80),
  postal_code         VARCHAR(12)  AS SUBSTR($1, 274, 12),
  city                VARCHAR(25)  AS SUBSTR($1, 286, 25),
  state_province      VARCHAR(20)  AS SUBSTR($1, 311, 20),
  country             VARCHAR(24)  AS SUBSTR($1, 331, 24),
  ceo_name            VARCHAR(46)  AS SUBSTR($1, 355, 46),
  description         VARCHAR(150) AS SUBSTR($1, 401, 150),
  qtr_start_date      VARCHAR(8)   AS SUBSTR($1, 31, 8),
  posting_date        VARCHAR(8)   AS SUBSTR($1, 39, 8),
  revenue             VARCHAR(17)  AS SUBSTR($1, 47, 17),
  earnings            VARCHAR(17)  AS SUBSTR($1, 64, 17),
  eps                 VARCHAR(12)  AS SUBSTR($1, 81, 12),
  diluted_eps         VARCHAR(12)  AS SUBSTR($1, 93, 12),
  margin              VARCHAR(12)  AS SUBSTR($1, 105, 12),
  inventory           VARCHAR(17)  AS SUBSTR($1, 117, 17),
  assets              VARCHAR(17)  AS SUBSTR($1, 134, 17),
  liabilities         VARCHAR(17)  AS SUBSTR($1, 151, 17),
  sh_out              VARCHAR(13)  AS
    IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 168, 13), SUBSTR($1, 127, 13)),
  diluted_sh_out      VARCHAR(13)  AS SUBSTR($1, 181, 13),
  co_name_or_cik      VARCHAR(60)  AS
    IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 194, 10), SUBSTR($1, 168, 10)),
  symbol              VARCHAR(15)  AS SUBSTR($1, 26, 15),
  issue_type          VARCHAR(6)   AS SUBSTR($1, 41, 6),
  name                VARCHAR(70)  AS SUBSTR($1, 51, 70),
  ex_id               VARCHAR(6)   AS SUBSTR($1, 121, 6),
  first_trade_date    VARCHAR(8)   AS SUBSTR($1, 140, 8),
  first_trade_exchg   VARCHAR(8)   AS SUBSTR($1, 148, 8),
  dividend            VARCHAR(12)  AS SUBSTR($1, 156, 12)
)
PARTITION BY (year,quarter)
LOCATION = @training_db.traininglab.ed_stage/finwire
FILE_FORMAT = (format_name = 'txt_fixed_width');


-- 13.4.3  Refresh the external table

ALTER EXTERNAL TABLE finwire REFRESH;


-- 13.4.4  Execute some queries and examine their profiles

SELECT co_name_or_cik,
       year,
       quarter,
       sum(revenue::number)
FROM finwire
WHERE rec_type='FIN' AND year='1967' AND quarter='3'
GROUP BY 1,2,3;

SELECT co_name_or_cik,
       year,
       quarter,
       SUM(revenue::number)
FROM finwire
WHERE rec_type='FIN' and year='1989' and quarter='3'
GROUP BY 1,2,3;


-- 13.4.5  Create a materialized view that filters on REC_TYPE = 'CMP'

CREATE OR REPLACE MATERIALIZED VIEW finwire_cmp AS
  SELECT TO_TIMESTAMP_NTZ(PTS,'YYYYMMDD-HH24MISS') AS PTS,
    rec_type,
    company_name,
    cik, status,
    industry_id,
    sp_rating,
    try_to_date(founding_date) as founding_date,
    addr_line1,
    addr_line2,
    postal_code,
    city,
    state_province,
    country,
    ceo_name,
    description
  FROM finwire
  WHERE rec_type = 'CMP';


-- 13.4.6  Create a materialized view that filters on REC_TYPE = 'FIN'

CREATE OR REPLACE MATERIALIZED VIEW finwire_fin AS
  SELECT TO_TIMESTAMP_NTZ(PTS,'YYYYMMDD-HH24MISS') AS PTS,
    REC_TYPE,
    TO_NUMBER(year,4,0) AS YEAR,
    TO_NUMBER(quarter,1,0) AS QUARTER,
    TO_DATE(qtr_start_date, 'YYYYMMDD') AS QTR_START_DATE,
    TO_DATE(posting_date, 'YYYYMMDD') AS POSTING_DATE,
    TO_NUMBER(revenue,15,2) AS REVENUE,
    TO_NUMBER(earnings,15,2) AS EARNINGS,
    TO_NUMBER(eps,10,2) AS EPS,
    TO_NUMBER(diluted_eps,10,2) AS DILUTED_EPS,
    TO_NUMBER(margin,10,2) AS MARGIN,
    TO_NUMBER(inventory,15,2) AS INVENTORY,
    TO_NUMBER(assets,15,2) AS ASSETS,
    TO_NUMBER(liabilities,15,2) AS LIABILITIES,
    TO_NUMBER(sh_out,13,0) AS SH_OUT,
    TO_NUMBER(diluted_sh_out,13,0) AS DILUTED_SH_OUT,
    co_name_or_cik
  FROM finwire
  WHERE rec_type = 'FIN';


-- 13.4.7  Create a materialized view that filters on REC_TYPE = 'SEC'

CREATE OR REPLACE MATERIALIZED VIEW finwire_sec AS
  SELECT TO_TIMESTAMP_NTZ(PTS,'YYYYMMDD-HH24MISS') AS PTS,
    rec_type,
    symbol,
    issue_type,
    status,
    name,
    ex_id,
    TO_NUMBER(sh_out,13,0) AS SH_OUT,
    TO_DATE(first_trade_date,'YYYYMMDD') AS FIRST_TRADE_DATE,
    TO_DATE(first_trade_exchg,'YYYYMMDD') AS FIRST_TRADE_EXCHG,
    TO_NUMBER(dividend,10,2) AS DIVIDEND,
    co_name_or_cik
  FROM finwire
  WHERE rec_type = 'SEC';


-- 13.4.8  SHOW the materialized views

SHOW MATERIALIZED VIEWS;


-- 13.4.9  Run a query using the FINWIRE_FIN materialized view

SELECT co_name_or_cik,
       year,
       quarter,
       SUM(revenue)
FROM finwire_fin
WHERE rec_type='FIN' AND year=1967 AND quarter=2
GROUP BY 1,2,3;


-- 13.4.10 View the query profile
--         Bring up the query profile for this last query and see if the results
--         are what you expected.

-- 13.4.11 Resize your warehouse to XSMALL

ALTER WAREHOUSE TAPIR_arch_wh SUSPEND;
ALTER WAREHOUSE TAPIR_arch_wh SET
  WAREHOUSE_SIZE = XSMALL;


-- 13.5.0  Key Takeaways
--         In this lab you have learned
--         - A materialized view is a pre-computed data set derived from a query
--         specification (the SELECT in the view definition) and stored for
--         later use.
--         - You can create the materialized view with a clustering key that
--         causes Snowflake to reorganize the data during the initial creation
--         of the materialized view.
--         - A materialized view can also be placed on an external table.

