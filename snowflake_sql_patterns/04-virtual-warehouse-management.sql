
-- 4.0.0   Virtual Warehouse Management
--         In this lab you will learn and practice the following:
--         - The key metrics available to monitor warehouse load and elapsed
--         time for all workload types including Data Loading, Transformation
--         and Consumption
--         - The metrics and methods to identify warehouse deployment issues
--         including undersized and oversized warehouses and opportunities to
--         combine workloads
--         - How to analyze batch jobs to identify the optimum warehouse size
--         - How to analyze workload and elapsed time by USER and ensure correct
--         warehouse deployment
--         - How to identify the average file sizes for COPY operations to help
--         right size the warehouse

-- 4.1.0   Part 1: Environment Set up

-- 4.1.1   Create Worksheet from SQL File and create schema and set your
--         context.
--         Part 1A: Firstly, set up environment:

USE ROLE arch_role;
CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh;
USE WAREHOUSE TAPIR_arch_wh;
CREATE DATABASE IF NOT EXISTS TAPIR_arch_db;
USE DATABASE TAPIR_arch_db;
CREATE OR REPLACE SCHEMA warehouses;
USE SCHEMA warehouses;

--         Produce a local copy of the statistics tables (normally from
--         SNOWFLAKE.ACCOUNT_USAGE)
--         The following is a sample set of statistics to illustrate the
--         concepts.

CREATE OR REPLACE TABLE query_history
   CLONE TRAINING_DB.USAGELAB.QUERY_HISTORY;

CREATE OR REPLACE TABLE copy_history
   CLONE TRAINING_DB.USAGELAB.COPY_HISTORY;


-- 4.1.2   Part 1B: Deploy UDFs for formatting.
--         The following User Defined Functions (UDFs) have been used to format
--         output in human readable form Execute the following SQL to deploy
--         these.

-- Format as bytes (1 decimal place)
create or replace function public.format_bytes (BYTES float)
  returns varchar
  language javascript
as
// Converts bytes into 50TB or 50GB or 50MB or 50K
// Used to format a number of bytes into human readable form
$$
var tb = 1024*1024*1024*1024;
var gb = 1024*1024*1024;
var mb = 1024*1024;
var kb = 1024;
var out = '';

if (BYTES > tb)  {
   out = out.concat(Math.trunc(BYTES/tb), 'TB');
} else if (BYTES > gb)  {
   out = out.concat(Math.trunc(BYTES/gb), 'GB');
} else if (BYTES > mb)  {
   out = out.concat(Math.trunc(BYTES/mb), 'MB');
} else if (BYTES > kb)  {
   out = out.concat(Math.trunc(BYTES/kb), 'KB');   
} else {
   out = Math.trunc(BYTES);
}
return out;
$$;

-- Format as NUMBER (1 decimal place)
create or replace function public.format_number (row_count number)
returns varchar
comment = 'Formats a numeric NUMBER into T, B, M or K'
as
'select
       case
         when row_count >= power(10, 12) then to_char(round(row_count / power(10, 12), 1)) || '' Trillion''
         when row_count >= power(10, 9)  then to_char(round(row_count / power(10, 9), 1))  || '' Billion''
         when row_count >= power(10, 6)  then to_char(round(row_count / power(10, 6), 1))  || '' Million''
         when row_count >= power(10, 3)  then to_char(round(row_count / power(10, 3), 1))  || '' K''
           else to_char(row_count)
        end as r_count'
;


-- Milliseconds to Time
--  Converts a number of milliseconds seconds into HH:MM:SS

create or replace function public.mseconds_to_time(MSECONDS double)
  returns varchar
  language javascript
as
// Converts a number of seconds into h:m:s
// Used to format a number of seconds into an elapsed time in human readable form
$$
var secs     = Math.trunc(MSECONDS / 1000);
var hrs       = Math.trunc(secs /60/60);
var mins    = Math.trunc((secs - (hrs*60*60))/60);
var f_secs  = Math.trunc((secs - (hrs*60*60))-mins*60);
var time     = '';

if (hrs > 0)  {
   time = time.concat(hrs, 'h ', mins, 'm ', f_secs,'s');
} else if (mins > 0)  {
      time = time.concat(mins, 'm ', f_secs,'s');
} else if (secs > 0)  {
   time = time.concat(secs,'s');
} else {
   time = time.concat(MSECONDS,'ms');
}
return time;
$$;

-- Credits Per Hour
-- Returns the number of nodes depending upon warehouse size

create or replace function public.credits_per_hour(SIZE varchar)
  returns double
  language javascript
as
// Returns the credits per hour charge for a given warehouse size
// eg. utl.node_count('4XLARGE') returns 128
$$
var size  = SIZE.toUpperCase();

if (size == 'X-SMALL')  {
   return 1;
} else if (size == 'SMALL')  {
   return 2;
} else if (size == 'MEDIUM')  {
   return 4;
} else if (size == 'LARGE')  {
   return 8;
} else if (size == 'X-LARGE')  {
   return 16;
} else if (size == '2X-LARGE')  {
   return 32;
} else if (size == '3X-LARGE')  {
   return 64;
} else if (size == '4X-LARGE')  {
   return 128;
} else if (size == '5X-LARGE')  {
   return 256;
} else if (size == '6X-LARGE')  {
   return 512;
} else {
   return 0;
}
$$;


-- 4.2.0   Part 2: Warehouse and Workload Measures - Query History
--         The elapsed time statistics for every SQL statement are available in
--         a Snowflake view under the SNOWFLAKE database and ACCOUNT_USAGE
--         schema. This is the recommended source of data as it contains a
--         history of all SQL statements over the past 12 months.
--         It is good practice to capture an incremental copy of these views to
--         central location. This enables trend analysis, for example to compare
--         the compute spend from the current vs previous year.
--         It is advisable however to take a local copy of the table to maximize
--         query performance when analyzing results.
--         In this case, we’ve provided two tables pre-populated with sample
--         data.
--         - QUERY_HISTORY - A sample of query statistics with some entries
--         modified to illustrate points made
--         - COPY_HISTORY - A sample of COPY statistics

-- 4.2.1   The following query lists the key measures from our sample
--         QUERY_HISTORY.

SELECT  query_id       
,       user_name                    
,       role_name                   
,       session_id               
,       query_text             
,       query_type             
,       warehouse_name         
,       warehouse_size         
,       total_elapsed_time     
,       execution_time         
,       queued_overload_time   
,       bytes_scanned          
,       percentage_scanned_from_cache   
,       partitions_scanned              
,       partitions_total                
,       bytes_spilled_to_local_storage  
,       bytes_spilled_to_remote_storage
,       rows_produced                  
,       rows_inserted                  
,       rows_updated                   
,       rows_deleted                   
FROM query_history
WHERE warehouse_size is not null
LIMIT 1000;

--         The result should show:
--         The view QUERY_HISTORY holds many values, but the most valuable for
--         our purposes include:
--         Query ID: A unique generated (internal) query ID
--         User Name: The name of the USER who executed the query
--         Role Name: The current_role() at the time the query was executed
--         Session ID: Every session is uniquely identified by a SESSION_ID -
--         useful to identify when JOBS are repeatedly executed
--         Query Text: The SQL text of the query
--         Query Type Type of Query. Including SELECT, INSERT, UPDATE, DELETE,
--         MERGE, COPY
--         Warehouse Name Warehouse Name
--         Warehouse Size The Size of the warehouse. EG. X-Small, Small, Medium
--         Total Elapsed Time Elapsed Time (1,000s of a second)
--         Execution Time Actual time spent executing query (1,000s of a
--         second). Excludes wait time (inc. Queuing)
--         Queued Overload Time Time spent queuing as warehouse overloaded
--         (1,000s of a second)
--         Bytes Scanned Total number of bytes scanned from all tables in this
--         query
--         Percentage from Cache Percentage of the data read from the Warehouse
--         Cache (SSD)
--         Partitions Scanned Number of Micro-Partitions scanned. If
--         PARTITIONS_SCANNED is a high percentage of PARTITIONS_TOTAL -
--         indicates at or near full table scan
--         Partitions Total Total number of micro-partitions in the table. If
--         significantly higher then Partitions Scanned - indicates good pruning
--         Bytes Spilled Local Storage Number of bytes spilled to local storage
--         (SSD) from memory (during sorts). Impacts query performance
--         Bytes Spill Remote Storage Number of bytes spilled to disk from SSD
--         (during sorts). Significant impact upon query performance
--         Rows Produced Number of rows returned to the result set
--         Rows Inserted Number of rows inserted
--         Rows Updated Number of rows updated
--         Rows Deleted Number of rows deleted

-- 4.3.0   Part 3: Right Sizing the Warehouse

-- 4.3.1   Part 3.1: Review workload size by virtual warehouse.
--         The following query reports the key metrics for queries by warehouse.
--         This can be used to help diagnose warehouse deployment issues and
--         help identify:
--         - Multiple same size warehouses with similar workloads - candidate to
--         combine
--         - Oversized warehouses where the Median and 90th percentile are low
--         and little spilling
--         - Undersized warehouses where the Median and 90th percentile are very
--         high and/or spilling
--         - Mixed workloads where the Median and 90th percentile have
--         significant range indicating the warehouse may be running a mix of
--         short and very long running queries with potential to identify and
--         move the more compute intensive processing to a larger warehouse
--         In the SQL below we have restricted queries to tagged purely for
--         demonstration purposes:

SELECT  
        warehouse_name
,       warehouse_size                                                                          
,       count(*)                                           AS count_queries                        
,       mseconds_to_time(median(total_elapsed_time))       AS median_elapsed_time                  
,       mseconds_to_time(percentile_cont(.90) within group(order by total_elapsed_time)) as p90_elapsed
,       mseconds_to_time(median(queued_overload_time))     AS median_overload_time                 
,       format_bytes(median(bytes_scanned))                AS median_bytes_scanned   
,       format_bytes(avg(bytes_spilled_to_local_storage))  avg_spilled_to_local_storage     
,       format_bytes(avg(bytes_spilled_to_remote_storage)) avg_spilled_to_remote_storage   
,       credits_per_hour(warehouse_size)                           AS wh_credits_hr
FROM query_history
WHERE warehouse_size is not null
AND query_tag is not null -- Remove this to show all warehouses
GROUP BY 1, 2
ORDER BY wh_credits_hr desc;

--         The results are:
--         The following points are worth noting about each warehouse in the
--         report:
--         BATCH_BIG: Virtual Warehouse is an 4X-Large (128 node) but has a
--         median and 90th percentile elapsed time of under 2 minutes. Although
--         the query count is low, this may indicate the workload on this
--         warehouse is too small for the warehouse. Consider moving this
--         workload to a smaller warehouse.
--         BATCH_MIDDLE Likewise is an 2X-Large size warehouse but 90% of
--         queries complete within 13 seconds. It’s worth questioning whether
--         the workload running could be moved to a smaller warehouse.
--         BATCH_STANDARD Is a Medium size (32 node) warehouse with a huge range
--         between the Median and 90th percentile elapsed time. In addition, the
--         queries caused spilling of 113GB of data indicating potentially large
--         sort operations on a warehouse that is too small.
--         The huge elapsed time range indicates there is perhaps a small number
--         of huge workloads running on this warehouse which produce the 90th
--         percentile elapsed of over 11 hours. If so, these workloads should be
--         tested for size and moved to a bigger warehouse.
--         ANALYST_TINY Is an X-Small warehouse with an elapsed time of between
--         2-6 minutes and well under a gigabyte of spilling to storage. This
--         indicates queries are correctly sized for this warehouse. If however
--         the expected performance of workloads is too slow, consideration
--         should be given to moving these to a SMALL warehouse. However, only
--         do this on a workload by workload basis if the move produces around a
--         50% reduction in elapsed time.
--         The most important measures to highlight from the results include:
--         Count Queries: Is important. If too low, this does not provide an
--         indicative result set
--         Median Elapsed Time: Indicates the mid-range elapsed time on this
--         warehouse
--         P90 Elapsed: Indicates the performance of the slowest 10% of queries.
--         90% of queries completed faster this this time.
--         Median Overload Time: Is the mid-range time queries where queued as
--         the warehouse was overloaded
--         Median Bytes Scanned: Is one of the available indicators of workload
--         size
--         AVG Spilled: Has a significant impact upon query performance.
--         Indicating spilling to storage
--         Warehouse Credits/Hour: Is a derived value and allows sorting by
--         warehouse credit rate

-- 4.3.2   Part 3.2: See the deployment by warehouse size.
--         The following query gives a birds-eye view of the warehouses by size.
--         It shows for each warehouse size, the number of warehouses deployed
--         and the key metrics.

SELECT  
        warehouse_size
,       count(distinct warehouse_name)                             AS count_warehouses
,       count(*)                                           AS count_queries                        
,       mseconds_to_time(median(total_elapsed_time))       AS median_elapsed_time                  
,       mseconds_to_time(percentile_cont(.90) within group(order by total_elapsed_time)) AS p90_elapsed               
,       format_bytes(median(bytes_scanned))                AS median_bytes_scanned   
,       format_bytes(avg(bytes_spilled_to_local_storage))  "AVG_SPILLED_LOCAL"     
,       format_bytes(avg(bytes_spilled_to_remote_storage)) "AVG_SPILLED_REMOTE"        
,       credits_per_hour(warehouse_size)                           AS wh_credits_hr
FROM query_history
WHERE warehouse_size is not null
AND query_tag is null
GROUP BY 1
ORDER BY wh_credits_hr desc;

--         The results are:
--         The most important aspect of the above results is the number of
--         warehouses at each size. In this case there are over 500 X-SMALL
--         warehouses and 150 SMALL warehouses. Any number of warehouses at each
--         size are potential candidates to combine workloads.
--         Running multiple warehouses at the same size clearly leads to credit
--         wastage unless the warehouses are constantly running at 100% of
--         available capacity.
--         The second point to make is it is only sensible to combine workloads
--         with a similar workload size. A good indicator at each T-Shirt size
--         is the Median and 90th percentile elapsed time. Clearly in this
--         (admittedly unrepresentative sample data) there is an argument to
--         combine all workloads to a smaller number of virtual warehouses.

-- 4.4.0   Part 4: Batch Job Deployment
--         As automated jobs typically account for 70% of credit spend, it is
--         important to correctly allocate batch jobs to a correctly size
--         warehouse to avoid contention for resources.

-- 4.4.1   The SQL query below assumes batch jobs are tagged with a QUERY_TAG
--         and reports the key measures to evaluate jobs by size.

SELECT  query_tag  
,       warehouse_name  
,       warehouse_size                   
,       mseconds_to_time(median(total_elapsed_time))       AS median_elapsed_time                  
,       mseconds_to_time(percentile_cont(.90) within group(order by total_elapsed_time)) AS p90_elapsed
,       format_bytes(median(bytes_scanned))                AS median_bytes_scanned                        
,       format_bytes(avg(bytes_spilled_to_local_storage))  avg_spilled_to_local_storage     
,       format_bytes(avg(bytes_spilled_to_remote_storage)) avg_spilled_to_remote_storage   
FROM query_history
WHERE warehouse_size is not null
AND   query_tag is not null
GROUP BY 1, 2, 3
ORDER BY query_tag, warehouse_name;

--         The result should show:
--         The important points to note in this report include:
--         ABC100 Completes in 2-5 minutes on an X-SMALL warehouse. Provided the
--         elapsed time for this job is acceptable, this seems well placed,
--         although moving the job to a SMALL warehouse may be an option to
--         consider.
--         ADG292 Completes on an 4X-LARGE warehouse in around 2 minutes without
--         any spilling which raises the question whether the elapsed time of
--         this job is critical and could it be executed on a smaller warehouse.
--         JAR400 Takes between 13-23 hours to complete on a MEDIUM size
--         warehouse spilling Gigabytes to local and remote storage. Clearly
--         this job is a candidate to run on a larger warehouse. The spilling to
--         remote storage is a particular concern as this adds significantly to
--         the elapsed time of the job (adding to credit consumption). It is
--         often cost effective to run workloads on a larger warehouse to reduce
--         spilling as this delivers results faster.
--         KHR003 Executes on a 2X-Large (32 nodes) virtual warehouse with a
--         90th percentile elapsed time of just 13 seconds. This is a candidate
--         to consider running on a smaller warehouse unless minimizing elapsed
--         time is absolutely critical.
--         SLR292 Executes on a MEDIUM size warehouse with a median elapsed time
--         of just 17 seconds. However, the 90th percentile is nearly 5 hours
--         and includes 103GB of spilling to storage. Given these measures are
--         for a single Job, this indicates perhaps a small number of queries
--         which could be executed on a larger warehouse. Consider identifying
--         these and executing a USE WAREHOUSE command to switch these
--         particularly demanding queries to a bigger warehouse - although this
--         should be an exceptional situation and only sensible in rare cases.
--         Remember to document the reason in the code for future reference.
--         NOTE: Notice that the Virtual Warehouse names indicate both the Type
--         and Size of workload. This is a good practice as it makes it easier
--         to identify the purpose.
--         Avoid naming warehouses by the standard Snowflake sizes as these may
--         be changed over time as warehouses are occasionally resized.

-- 4.5.0   Part 5: Monitoring Batch Job History
--         Unlike user workloads which tend to vary, batch jobs tend to follow a
--         repeating trend although the actual work done each run may vary. It
--         is therefore important to monitor batch jobs over time to verify they
--         are not running on oversized or undersized virtual warehouses.
--         It can also be useful to track elapsed time compared to average size
--         of work completed (EG. Bytes scanned or rows inserted, updated and
--         deleted). This can be used (for example) to detect a sudden change in
--         performance profile without a corresponding change in work done. This
--         might indicate a design or code change that has significantly
--         impacted query performance.

-- 4.5.1   The following query uses the SESSION_ID and START_TIME to indicate
--         the start of each job, and shows the statistics over a period of
--         time.

SELECT  session_id
,       to_char(min(start_time),'DD-MON-YYYY HH24:MI')     AS "Start Time"
,       query_tag                                          AS "Job ID"
,       warehouse_name                                     AS "Warehouse"
,       warehouse_size                                     AS "Warehouse Size"          
,       mseconds_to_time(median(total_elapsed_time))       AS "Median Elapsed"                  
,       mseconds_to_time(percentile_cont(.90) within group(order by total_elapsed_time)) AS "90th Elapsed"
,       format_bytes(median(bytes_scanned))                AS "Median Bytes Scanned"                        
,       format_bytes(avg(bytes_spilled_to_local_storage))  "Avg Spill Local"     
,       format_bytes(avg(bytes_spilled_to_remote_storage)) "Avg Spill Remote"
,       mseconds_to_time(sum(total_elapsed_time))          AS "Total Elapsed"  
,       format_bytes(sum(bytes_scanned))                   AS "Bytes Scanned"
,       sum(rows_inserted + rows_updated + rows_deleted)   AS "Ins+Upd+Del"
FROM query_history
WHERE warehouse_size is not null
AND   query_tag = 'SLR292'
GROUP BY 1, 3, 4, 5
ORDER BY query_tag, session_id;

--         The result should show:
--         Looking at the results, it is clear that the workload only really
--         settled into a pattern after 31-Aug-2020. After this time, we see the
--         90th Percentile rising to nearly 24 hours and as much as a terabyte
--         spilled to storage.
--         This indicates that the workload on this job has perhaps increased
--         significantly - perhaps as a result of additional SQL steps. The end
--         result, is the situation should be investigated and either the long
--         running SQL or the entire job moved to a larger warehouse.

-- 4.6.0   Part 6: Monitoring User Workloads
--         The same measures apply to user workloads although the method used to
--         group users together differs from batch jobs. In this case, it’s
--         sensible to design a purpose built roll-up table to aggregate users
--         by team.

-- 4.6.1   In this case, for simplicity, we simply list the statistics for each
--         user.

SELECT  user_name   
,       warehouse_name
,       warehouse_size
,       count(*)                                           AS count_queries                        
,       mseconds_to_time(median(total_elapsed_time))       AS median_elapsed_time                  
,       mseconds_to_time(percentile_cont(.90) within group(order by total_elapsed_time)) AS p90_elapsed
,       format_bytes(median(bytes_scanned))                AS median_bytes_scanned                        
,       format_bytes(avg(bytes_spilled_to_local_storage))  avg_spilled_to_local_storage     
,       format_bytes(avg(bytes_spilled_to_remote_storage)) avg_spilled_to_remote_storage   
FROM query_history
WHERE warehouse_size is not null
AND   query_tag is not null -- Note: For demo purposes only - to highlight specific results
-- and   query_tag is null  -- Exclude batch jobs
GROUP BY 1, 2, 3
ORDER BY warehouse_name, user_name;

--         The result should show:
--         The key points to note include:
--         ANALYST_TINY Six users are running queries on this warehouse and all
--         have an elapsed time from 41s to 10 minutes with spilling evident but
--         well under the gigabyte range. This appears acceptable.
--         BATCH_BIG One user is executing queries on this 4X-Large warehouse
--         (128 credits per hour) with a 90th percentile of around 2 minutes and
--         no spilling. This is a cause for concern. It may be that a user is
--         accidentally using this warehouse to run relatively small queries. If
--         this report excludes registered batch jobs, it may highlight misuse
--         of resources.
--         BATCH_STANDARD Two users are executing queries on this Medium Size
--         warehouse and both appear to have very long (23+ hours) elapsed
--         times. Either these are again users on a batch warehouse or batch
--         jobs being executed without correctly recording the JOB ID in the
--         query tag. They are potential candidates to move to a larger
--         warehouse to avoid contention for resources on this warehouse.

-- 4.7.0   Part 7: Monitoring File Sizes for COPY operations
--         When loading data using a COPY command, it is important to understand
--         the file size and number of files being loaded to make the most
--         efficient use of resources.
--         In this section we will demonstrate SQL to help monitor file sizes.

-- 4.7.1   Part 7.1: Now let’s look at overall average file size.
--         The SQL below gives a high level overview of file size and is a good
--         starting point.

SELECT format_bytes(avg(file_size)) AS avg_file_size
,    format_bytes(sum(file_size)) AS total_file_size
,    trunc(avg(file_size))      AS avg_file_size_bytes
,    trunc(sum(file_size))      AS total_file_size_bytes
FROM    copy_history
WHERE   status in ('Loaded', 'Partially loaded')
AND    pipe_name is null -- Ignore Snowpipe
ORDER BY avg_file_size_bytes desc;

--         The result should show:
--         The result often indicates the average file size is relatively low.
--         Certainly less than the recommended file size of between 100-250MB of
--         compressed data.
--         This is not an issue, but it does indicate that many COPY operations
--         are more efficiently executed on an X-SMALL warehouse.

-- 4.7.2   Part 7.2: Next breakdown by file size.
--         The SQL below breaks down the overall result into bands.
--         - Under 100MB
--         - Between 100-250MB
--         - Over 250MB
--         These give a better indicator of the size profile.

SELECT format_bytes(avg(case when file_size <= 104857600 then file_size end)) AS "Avg under 100MB"
,      count(case when file_size <= 104857600 then file_size end)             AS "Count under 100MB"
,      format_bytes(avg(case when file_size between 104857600 and 262144000 then file_size end)) AS "Avg 100-250MB"
,      count(case when file_size between 104857600 and 262144000  then file_size end)            AS "Count 100-250MB"
,      format_bytes(avg(case when file_size > 262144000 then file_size end)) AS "Avg over 250MB"
,      count(case when file_size > 262144000 then file_size end)              AS "Count over 250MB"
FROM copy_history;

--         The result should show:
--         The results indicate the majority of queries are under 100MB
--         (439,677) while less than 1% of files are over 250MB and these have
--         an average size of under 600MB.

-- 4.7.3   Part 7.3: Identify the largest file loads.
--         Having identified there are potentially large files which could be
--         split into smaller chunks to load faster, the following SQL drills
--         down further to identify the largest loads.

SELECT table_catalog_name           AS database
,    table_schema_name              AS schema_name
,    table_name                     AS table_name
,    count(*)                       AS count
,    format_bytes(avg(file_size))   AS avg_file_size
,    format_bytes(sum(file_size))   AS total_file_size
,    trunc(avg(file_size))          AS avg_file_size_bytes
,    trunc(sum(file_size))          AS total_file_size_bytes
FROM    copy_history
WHERE   status in ('Loaded', 'Partially loaded')
AND    pipe_name is null -- Ignore Snowpipe
AND    file_size > 262144000
GROUP BY 1, 2, 3
ORDER BY avg_file_size_bytes desc;

--         The result should show:
--         The results show the name of each Table, the number of files loaded
--         and average file size for loads with files over 250MB in size.
--         While these are the candidates to split and run on a larger sized
--         warehouse, it is only sensible if there is a need to reduce the
--         elapsed time. The next step would include checking the elapsed time
--         of loads.

-- 4.8.0   Key Takeaways
--         - There are over 21 important measures in QUERY_HISTORY which can be
--         used to track workload size and elapsed time
--         - The key metrics include the Median and 90th Percentile as these are
--         representative of both the elapsed time experienced and (for a given
--         warehouse size) an estimate of the workload
--         - It is important to analyze the warehouse workload depending upon
--         whether data is being loaded, transformed or consumed by end-users
--         - Different techniques are available to monitor the workload and
--         these give some useful insights
--         The most important takeaway from this exercise is however, it is
--         impossible to achieve a perfectly efficient workload across all
--         virtual warehouses. However, there are techniques and an overall
--         framework to help achieve an efficient deployment.

