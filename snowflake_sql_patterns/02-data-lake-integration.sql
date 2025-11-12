
-- 2.0.0   Data Lake Integration
--         By the end of this lab, you will be able to:
--         - Create and refresh external tables
--         - Query External Tables both Non-Partitioned and Partition
--         - See the effect of pruning against an external file
--         - Compare the query profile statistics for External Partitioned /
--         Non-Partitioned
--         - Loading Parquet files into a table with a VARIANT column
--         - Create a View over the table with a VARIANT column

-- 2.1.0   Set context and create DATABASE and SCHEMA

-- 2.1.1   Create Worksheet from SQL File and set your context.

USE ROLE arch_role;
ALTER SESSION SET use_cached_result = FALSE;
CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh WITH
    WAREHOUSE_SIZE = 'XSMALL'
    INITIALLY_SUSPENDED = TRUE
    AUTO_SUSPEND = 60;

CREATE DATABASE IF NOT EXISTS TAPIR_arch_db;
CREATE SCHEMA IF NOT EXISTS TAPIR_arch_db.Data_Lake;
USE SCHEMA TAPIR_arch_db.Data_Lake;
USE WAREHOUSE TAPIR_arch_wh;


-- 2.2.0   Working with External Data Lake Cloud Storage
--         Before starting this, we need to ensure we have files to work with.
--         An external stage has been pre-configured for this exercise. To check
--         that the files are available, run the following SQL.

-- 2.2.1   List the staged files.

LIST @training_db.traininglab.ed_stage/finwire;

--         This shows a list of files available and should look like this:
--         These files are of a fixed width nature, there should be about 203
--         files.

-- 2.2.2   Now to create a file format for this type of file.

-- Create fixed width file format
CREATE OR REPLACE FILE FORMAT TAPIR_arch_db.data_lake.txt_fixed_width
  TYPE = CSV
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = NONE
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 0
  TRIM_SPACE = FALSE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  NULL_IF = ('\\N');

--         The parameters that determine fixed width is:
--         - FIELD_DELIMITER = NONE
--         - RECORD_DELIMITER = \n (Each record is on a new line)

-- 2.2.3   Now the external table can be built. The following SQL creates the
--         external table.

--  Create an external table
CREATE OR REPLACE EXTERNAL TABLE finwire
  LOCATION = @training_db.traininglab.ed_stage/finwire
  REFRESH_ON_CREATE = TRUE
  FILE_FORMAT = (FORMAT_NAME = 'txt_fixed_width');  

--         Some explanation of these parameters:
--         - LOCATION = @training_db.traininglab.ed_stage/finwire
--         This is where the files exist. @training_db.training.ed_stage is a
--         stage object pointing to an AWS S3 bucket. /finwire is the folder in
--         which the files have been placed.
--         Note: As no PATTERN= parameter is specified, it’s now assumed that
--         all files in this folder are same format. Mixing different files in
--         this folder would cause issues.
--         - REFRESH_ON_CREATE = TRUE
--         When this value is set to TRUE, snowflake updates its own internal
--         metadata with the names of the files in the folder at the time the
--         external table is created. If additional files were to be added at a
--         later stage, the command ALTER TABLE finwire REFRESH; would be
--         required to ensure the new files are loaded into the metadata.
--         When this is set to FALSE, snowflake will not refresh the external
--         table metadata. The command ALTER TABLE finwire REFRESH; will be
--         required to update the metadata accordingly.
--         - AUTO_REFRESH = (TRUE | FALSE )
--         Although not mentioned in the above SQL, this is an interesting
--         parameter. Its purpose is to trigger a metadata refresh whenever a
--         new file lands in the location pointed to by an external table.
--         However, it is more complex than setting this parameter to TRUE.
--         You’ll also need to configure event notifications on your cloud
--         storage to notify Snowflake whenever new or updated data is
--         available.
--         Refer to Create External Table for more information.

-- 2.2.4   Let’s take a look at this table via the SHOW command.

SHOW TABLES LIKE 'finwire';

--         The output of this command should show this:
--         Here the column is_external tells us that the table is external.

-- 2.2.5   Another method to show only external tables would be to use the
--         following SQL.

SHOW EXTERNAL TABLES;

--         The output of this command tells us more details about the external
--         table:
--         The columns of interest to use here are:
--         - stage (Name of the Snowflake stage object)
--         - location (The location of the files)
--         - file_format_name (What file format object is being used)
--         This command is invaluable when dealing with external tables.
--         Please note that no columns have been named in this external table.

-- 2.2.6   Let’s query the external table to see what it looks like.

SELECT *
FROM finwire
LIMIT 10;

--         The output should look like this:
--         Not the best looking table output!!

-- 2.2.7   To help make this look better and more table like, lets define the
--         virtual columns for the external table.

-- Rebuild the External Table to break out columns
CREATE OR REPLACE EXTERNAL TABLE finwire
    (
     pts                VARCHAR(15)  AS SUBSTR($1, 8, 15),
     rec_type           VARCHAR(3)   AS SUBSTR($1, 23, 3),
     company_name       VARCHAR(60)  AS SUBSTR($1, 26, 60),
     cik                VARCHAR(10)  AS SUBSTR($1, 86, 10),
     status             VARCHAR(4)   AS IFF(SUBSTR($1, 23, 3) = 'CMP', SUBSTR($1, 96, 4),SUBSTR($1, 47, 4)),
     industry_id        VARCHAR(2)   AS SUBSTR($1, 100, 2),
     sp_rating          VARCHAR(4)   AS SUBSTR($1, 102, 4),
     founding_date      VARCHAR(8)   AS SUBSTR($1, 106, 8),
     addr_line1         VARCHAR(80)  AS SUBSTR($1, 114, 80),
     addr_line2         VARCHAR(80)  AS SUBSTR($1, 194, 80),
     postal_code        VARCHAR(12)  AS SUBSTR($1, 274, 12),
     city               VARCHAR(25)  AS SUBSTR($1, 286, 25),
     state_province     VARCHAR(20)  AS SUBSTR($1, 311, 20),
     country            VARCHAR(24)  AS SUBSTR($1, 331, 24),
     ceo_name           VARCHAR(46)  AS SUBSTR($1, 355, 46),
     description        VARCHAR(150) AS SUBSTR($1, 401, 150),
     year               VARCHAR(4)   AS SUBSTR($1, 8, 4),
     quarter            VARCHAR(1)   AS SUBSTR($1, 30, 1),
     qtr_start_date     VARCHAR(8)   AS SUBSTR($1, 31, 8),
     posting_date       VARCHAR(8)   AS SUBSTR($1, 39, 8),
     revenue            VARCHAR(17)  AS SUBSTR($1, 47, 17),
     earnings           VARCHAR(17)  AS SUBSTR($1, 64, 17),
     eps                VARCHAR(12)  AS SUBSTR($1, 81, 12),
     diluted_eps        VARCHAR(12)  AS SUBSTR($1, 93, 12),
     margin             VARCHAR(12)  AS SUBSTR($1, 105, 12),
     inventory          VARCHAR(17)  AS SUBSTR($1, 117, 17),
     assets             VARCHAR(17)  AS SUBSTR($1, 134, 17),
     liabilities        VARCHAR(17)  AS SUBSTR($1, 151, 17),
     sh_out             VARCHAR(13)  AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 168, 13), SUBSTR($1, 127, 13)),
     diluted_sh_out     VARCHAR(13)  AS SUBSTR($1, 181, 13),
     co_name_or_cik     VARCHAR(60)  AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 194, 10), SUBSTR($1, 168, 10)),
     symbol             VARCHAR(15)  AS SUBSTR($1, 26, 15),
     issue_type         VARCHAR(6)   AS SUBSTR($1, 41, 6),
     name               VARCHAR(70)  AS SUBSTR($1, 51, 70),
     ex_id              VARCHAR(6)   AS SUBSTR($1, 121, 6),
     first_trade_date   VARCHAR(8)   AS SUBSTR($1, 140, 8),
     first_trade_exchg  VARCHAR(8)   AS SUBSTR($1, 148, 8),
     dividend           VARCHAR(12)  AS SUBSTR($1, 156, 12)
    )
LOCATION = @training_db.traininglab.ed_stage/finwire
FILE_FORMAT = (format_name = 'txt_fixed_width');


-- 2.2.8   Now lets query the external table.

SELECT *
FROM finwire
LIMIT 10;

--         The output should look like this:
--         We can now see a much more table like output.
--         Notice the additional column VALUE - which holds the raw data.
--         Recap So Far We have created a non-partition table to examine data
--         held on cloud storage.

-- 2.2.9   Lets run a query against this table with a where clause.

SELECT year,
       quarter,
       sum(revenue::number)as total_revenue
FROM finwire  
WHERE year='2017'
AND   quarter = '1'
AND   rec_type='FIN'
GROUP BY 1,2
ORDER BY year, quarter;

--         The output should look like this:
--         Looking at the query profile for this:
--         It’s clear that all partitions were scanned, 203 out of 203.

-- 2.2.10  Let’s create a partitioned external table.

-- Rebuild the External Table
-- Including the clause: the PARTITION BY (year,quarter)
--
CREATE OR REPLACE EXTERNAL TABLE finwire_partitioned
(
    year                VARCHAR(4)   AS SUBSTR(METADATA$FILENAME, 16, 4),
    quarter             VARCHAR(1)   AS SUBSTR(METADATA$FILENAME, 21, 1),
    thestring           VARCHAR(90)  AS SUBSTR(METADATA$FILENAME, 1, 50),
    pts                 VARCHAR(15)  AS SUBSTR($1, 8, 15),
    rec_type            VARCHAR(3)   AS SUBSTR($1, 23, 3),
    company_name        VARCHAR(60)  AS SUBSTR($1, 26, 60),
    cik                 VARCHAR(10)  AS SUBSTR($1, 86, 10),
    status              VARCHAR(4)   AS IFF(SUBSTR($1, 23, 3) = 'CMP', SUBSTR($1, 96, 4),SUBSTR($1, 47, 4)),
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
    sh_out              VARCHAR(13)  AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 168, 13), SUBSTR($1, 127, 13)),
    diluted_sh_out      VARCHAR(13)  AS SUBSTR($1, 181, 13),
    co_name_or_cik      VARCHAR(60)  AS IFF(SUBSTR($1, 23, 3) = 'FIN', SUBSTR($1, 194, 10), SUBSTR($1, 168, 10)),
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

--         Note: This table is partitioned by year and quarter. We can not use
--         the offsets within the file for year and quarter, they must come from
--         the filename. The reason for this is that the filenames contain the
--         year and quarter. E.g. One of the file names is
--         s3://snowflakeed/finwire/FINWIRE1967Q1, here you can see the year is
--         1967 and the quarter is Q1. From this, we can extract the year number
--         and quarter number.

-- 2.2.11  Now lets run the same query with the filter.

SELECT year,
       quarter,
       sum(revenue::number)as total_revenue
FROM finwire_partitioned  
WHERE year='2017'
AND   quarter = '1'
AND   rec_type='FIN'
GROUP BY 1,2
ORDER BY year, quarter;

--         Looking at the query profile for this:
--         Here we see only one partition is used.
--         Note: Do not confuse the above partition with snowflake micro-
--         partition. In this context, the partition is a grouping by files by
--         year and quarter, in this case its one file.

-- 2.3.0   Raw History in Snowflake
--         Now we will work this storing the raw_history data inside of
--         Snowflake. To do this, we will work with parquet files. The parquet
--         file is optional but helpful in working with another file format.
--         We will perform the following steps:
--         Generate the parquet files
--         Create a table to store the raw data
--         Load the raw data into the table
--         Query the raw history table
--         Create a view over the raw history table
--         Step 1: Generate the parquet files This involves creating parquet
--         files from the finware_partitioned table that we created earlier.

-- 2.3.1   Before generating the parquet files, lets run some sql to ensure that
--         the folder is emptied.

REMOVE @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ARCHITECT/TAPIR/parquet/finwire;

--         If no files exist in this folder, you may get the message Query
--         Produced No Results, this is normal

-- 2.3.2   To generate the parquet files, use the following SQL.

COPY INTO
@TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ARCHITECT/TAPIR/parquet/finwire
  FROM (SELECT * FROM finwire_partitioned)
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.myparquetformat)
  HEADER=TRUE;

--         Note: The parquet files are deployed via the pre-define stage object
--         @TRAINING_DB.TRAININGLAB.CLASS_STAGE The parquet files will be placed
--         in the folder COURSE/ARCHITECT/TAPIR/parquet/finwire The file
--         format has been pre-defined HEADER=TRUE ensures the the column names
--         are generated in the parquet files, without this parameter the
--         columns get named col1, col2 …

-- 2.3.3   Let’s now list the files generated.

LIST @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ARCHITECT/TAPIR/parquet/finwire;

--         The output should look like the following:
--         Note The output shows 8 files, approximately 10MB in size.

-- 2.3.4   Let’s create a simple table with a VARIANT type.

CREATE TABLE IF NOT EXISTS finwire_raw_history (
data    VARIANT
);


-- 2.3.5   Now load the parquet files into this table. The following SQL will
--         perform that task.

copy into finwire_raw_history
from @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ARCHITECT/TAPIR/parquet/finwire
  FILE_FORMAT=(FORMAT_NAME=training_db.traininglab.myparquetformat);

--         The output received from this command should look like this:
--         This output is of great interest as it informs you of the following
--         details:
--         - Status (The status of each file that is loaded. LOADED => Success)
--         - Rows_Parsed (The number of rows in the file)
--         - Rows_Loaded (The actual number of rows that were loaded, if all is
--         well this number should match Rows_Parsed)
--         - Error_Limit (This is the number of errors that are allowed before
--         the load of the file is terminated)
--         - Errors_Seen (The number of errors encountered while loading)
--         - First_Error (Provides details on the first error encountered)
--         - First_Error_Line (This is the row number of the file where the
--         error was encountered)
--         - First_Error_Character (The character in which the first error
--         occurred)
--         - First_Error_Column_Name (The Column in which the first error was
--         encountered)
--         The Error columns are invaluable in understanding where any errors
--         occur.

-- 2.3.6   Let’s query the table. The following SQL can be used.

-- Demonstrate the data is in Snowflake in VARIANT format
SELECT *
FROM finwire_raw_history
LIMIT 100;

--         The output should look like the following:
--         Note
--         At this point we now see all the data that has been loaded, there is
--         no concept of which file each row of data has come from.
--         Each row in the table represents a row of parquet data.
--         The format is not too friendly at this stage.
--         Clicking on a row will show the content of the parquet row. E.g.
--         Here, we can clearly see the name value pairs

-- 2.3.7   To view the data in a more table like manner, the following query can
--         be used.

-- Extract out the columns
SELECT  rtrim(data:"YEAR"::number)      as YEAR
,       rtrim(data:"QUARTER"::number)   as QUARTER
,       rtrim(data:"REVENUE"::number)   as REVENUE
,       rtrim(data:"REC_TYPE"::string)  as REC_TYPE
FROM finwire_raw_history f
WHERE REC_TYPE = 'FIN'
LIMIT 100;

--         Note: rtrim has been placed in the query to remove any trailing
--         spaces. Another method would be to specify TRIM_SPACE = TRUE in the
--         file format object. As the file format object that was pre-defined,
--         did not have this, the rtrim function was used.
--         The output should look like this:
--         This is now more table like.
--         However, we should take this further and create a view over the table
--         to allow more friendly SQL queries.

-- 2.3.8   To create the view, the following SQL can be used.

-- Create as a view
CREATE OR REPLACE VIEW finwire_internal as
select  rtrim(data:"YEAR"::number)      as YEAR
,       rtrim(data:"QUARTER"::number)   as QUARTER
,       rtrim(data:"REVENUE"::number)   as REVENUE
,       rtrim(data:"REC_TYPE"::string)  as REC_TYPE
FROM finwire_raw_history f
WHERE REC_TYPE = 'FIN';


-- 2.3.9   And now, let’s perform a SQL query with some additional filters in
--         the WHERE clause.

SELECT year,
       quarter,
       sum(revenue::number) as total_revenue
FROM finwire_internal
WHERE year='2017'
AND   quarter = '1'
AND   rec_type='FIN'
GROUP BY 1,2
ORDER BY year, quarter;

--         The output should look like this:
--         Note: You may have noticed that this is the same result as when you
--         executed a similar query against the CSV files.

-- 2.3.10  Tidy Up.
--         Lets clean up the work we have done. The following SQL will perform
--         the tidy up

DROP FILE FORMAT TAPIR_arch_db.data_lake.txt_fixed_width;
DROP TABLE finwire;
DROP TABLE finwire_partitioned;
DROP TABLE finwire_raw_history;
DROP VIEW finwire_internal;
REMOVE @TRAINING_DB.TRAININGLAB.CLASS_STAGE/COURSE/ARCHITECT/TAPIR/parquet/finwire;


-- 2.4.0   Key Takeaways
--         In this lab you have learned
--         - The difference using raw data that exists on the cloud platform
--         verses loading raw data into snowflake, the pro’s and con’s of each.
