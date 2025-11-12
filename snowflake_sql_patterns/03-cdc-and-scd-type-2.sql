
-- 3.0.0   CDC and SCD Type 2
--         By the end of this lab, you will be able to:
--         - Create a Table and a History Table.
--         - Create a Stream.
--         - Use a Merge Statement to handle changes into the history table.
--         - Insert/Update/Delete Rows and see the history.
--         Change data capture (CDC) is an approach to data integration based on
--         identifying, capturing, and delivering the changes made to enterprise
--         data sources.
--         A Slowly Changing Dimension (SCD) is a dimension that stores and
--         manages both current and historical data over time in a data
--         warehouse. The Slowly Changing Dimension Type 2 is used to maintain
--         the complete history of a record in a target.
--         Note that a stream itself does not contain any table data. A stream
--         only stores an offset for the source object and returns CDC records
--         by leveraging the versioning history for the source object. When the
--         first stream for a table is created, several hidden columns are added
--         to the source table and begin storing change-tracking metadata. These
--         columns consume a small amount of storage. The CDC records returned
--         when querying a stream rely on a combination of the offset stored in
--         the stream and the change tracking metadata stored in the table.
--         The Database, schemas, and objects in this diagram are utilized in
--         this lab.
--         Data is inserted / modified in the table named golfers. A stream
--         called Golfers_Changes is created such that any changes to the
--         Golfers table will be captured.
--         The Golfer_History table is where SCD2 type records are stored.
--         The view Golfers_Change_Data combines data from the stream and
--         Golfers_History to determines the action required based on
--         insert/update or delete action against the underlying golfers table.
--         The following lab shows how to implement this design in Snowflake.

-- 3.1.0   Setup Lab Environment
--         In this exercise you will setup a database, schema, and tables that
--         will be used for all the exercises in this lab.

-- 3.1.1   Create Worksheet from SQL File and create schema and set your
--         context.

USE ROLE arch_role;

CREATE DATABASE IF NOT EXISTS TAPIR_arch_db;
CREATE SCHEMA IF NOT EXISTS TAPIR_arch_db.cdc_lab;
USE SCHEMA TAPIR_arch_db.cdc_lab;

CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh;
USE WAREHOUSE TAPIR_arch_wh;


-- 3.2.0   Create the tables
--         Use your worksheet and SQL to create the tables.

-- 3.2.1   First, create a table you will use to contain the data.
--         This table does need Update_Timestamp column. This reflects when a
--         row was inserted/updated or deleted.

CREATE OR REPLACE TABLE Golfers
(Golfer_ID   INTEGER
,First_Name   VARCHAR(100)
,Last_Name    VARCHAR(100)
,Middle_Initial  VARCHAR(1)
,Date_Of_Birth DATE
,Email_Address Varchar(100)
,Update_Timestamp timestamp_ntz);


-- 3.2.2   Next create the history table.

CREATE OR REPLACE TABLE Golfers_History
(Golfer_ID   INTEGER
,First_Name   VARCHAR(100)
,Last_Name    VARCHAR(100)
,Middle_Initial  VARCHAR(1)
,Date_Of_Birth DATE
,Email_Address Varchar(100)
,Start_Time  timestamp_ntz
,End_Time    timestamp_ntz
,Current_Flag integer);


-- 3.3.0   Create a Stream

-- 3.3.1   Now create a stream on the table Golfers.

CREATE OR REPLACE STREAM Golfers_changes ON TABLE Golfers;


-- 3.3.2   Let’s take a look at the stream.

SHOW STREAMS;


-- 3.3.3   Query the stream, it should show no rows.

SELECT * FROM Golfers_Changes;


-- 3.4.0   Have a View to assist with the Merge statement

-- 3.4.1   Create a view to assist with the Merge statement.

CREATE OR REPLACE VIEW Golfers_change_data AS
-- This subquery figures out what to do when data is inserted into the Golfers table
-- An insert to the Golfers table results in an INSERT to the GOLFERS_HISTORY table
SELECT Golfer_ID  
      ,First_Name   
      ,Last_Name    
      ,Middle_Initial
      ,Date_Of_Birth
      ,Email_Address
      ,start_time
      ,end_time
      ,current_flag
      ,'I' as dml_type
FROM (select Golfer_ID  
            ,First_Name   
            ,Last_Name    
            ,Middle_Initial
            ,Date_Of_Birth
            ,Email_Address
            ,Update_Timestamp as start_time
            ,lag(Update_Timestamp) over (partition by Golfer_ID order by Update_Timestamp desc) as end_time_raw
            ,case
                when end_time_raw is null then '9999-12-31'::timestamp_ntz
                else end_time_raw
             end as end_time
            ,case  
                when end_time_raw is null then 1
                else 0
             end as current_flag
       FROM (select Golfer_ID  
                   ,First_Name   
                   ,Last_Name    
                   ,Middle_Initial
                   ,Date_Of_Birth
                   ,Email_Address
                   ,Update_Timestamp
             FROM Golfers_Changes
             WHERE metadata$action = 'INSERT'
             AND metadata$isupdate = 'FALSE'))
UNION
-- This subquery figures out what to do when data is updated in the Golfers table
-- An update to the Golfers table results in an update AND an insert to the Golfers_HISTORY table
-- The subquery below generates two records, each with a different dml_type
SELECT Golfer_ID  
      ,First_Name   
      ,Last_Name    
      ,Middle_Initial
      ,Date_Of_Birth
      ,Email_Address
      ,start_time
      ,end_time
      ,current_flag
      ,dml_type
FROM (select Golfer_ID  
            ,First_Name   
            ,Last_Name    
            ,Middle_Initial
            ,Date_Of_Birth
            ,Email_Address
            ,update_timestamp as start_time
            ,lag(update_timestamp) over (partition by Golfer_ID order by update_timestamp desc) as end_time_raw
            ,case
                when end_time_raw is null then '9999-12-31'::timestamp_ntz
                else end_time_raw
              end as end_time
            ,case
               when end_time_raw is null then 1
               else 0
             end as current_flag
            ,dml_type
      FROM (-- Identify data to insert into golfers_history table
            SELECT Golfer_ID  
                  ,First_Name   
                  ,Last_Name    
                  ,Middle_Initial
                  ,Date_Of_Birth
                  ,Email_Address
                  ,update_timestamp
                  ,'I' as dml_type
            FROM golfers_changes
            WHERE metadata$action = 'INSERT'
            AND metadata$isupdate = 'TRUE'
            UNION
            -- Identify data in Golfers_HISTORY table that needs to be updated
            SELECT Golfer_ID
                  ,null
                  ,null
                  ,null
                  ,null
                  ,null
                  ,start_time
                  ,'U' as dml_type
            FROM golfers_history
            WHERE Golfer_ID in (SELECT DISTINCT Golfer_ID
                                  FROM Golfers_changes
                                  WHERE metadata$action = 'INSERT'
                                    AND metadata$isupdate = 'TRUE')
              AND current_flag = 1))
UNION
-- This subquery figures out what to do when data is deleted from the Golfers table
-- A deletion from the Golfers table results in an update to the Golfers_HISTORY table
SELECT gc.Golfer_Id
    , null
    , null
    , null
    , null
    , null
    , gh.start_time
    , current_timestamp()::timestamp_ntz
    , null
    ,'D' as dml_type
FROM Golfers_history gh
INNER JOIN Golfers_Changes gc on gh.Golfer_ID = gc.Golfer_ID
WHERE   gc.metadata$action = 'DELETE'
  AND   gc.metadata$isupdate = 'FALSE'
  AND   gh.current_flag = 1;


-- 3.4.2   Test the view, no rows should appear at this time.

SELECT * FROM Golfers_change_data;


-- 3.5.0   Insert Some data

-- 3.5.1   Now let’s insert some data.

SET update_timestamp = current_timestamp()::timestamp_ntz;

BEGIN;

INSERT INTO Golfers VALUES
 (1,'Arnold','Palmer',NULL,to_date('1929-09-10','YYYY-MM-DD'), 'arnold.palmer@snowflakeuni.com',$update_timestamp)
,(2,'Ben','Hogan',NULL,to_date('1984-05-21','YYYY-MM-DD'), 'ben.hogan@snowflakeuni.com',$update_timestamp)
,(3,'Greg','Norman',NULL,to_date('1955-02-10','YYYY-MM-DD'), 'greg.norman@snowflakeuni.com',$update_timestamp)
,(4,'Dustin','Johnson',NULL,to_date('1981-09-13','YYYY-MM-DD'), 'dustin.johnson@snowflakeuni.com',$update_timestamp)
,(5,'Bernhard','Langer',NULL,to_date('1957-08-27','YYYY-MM-DD'), 'bernhard.langer@snowflakeuni.com',$update_timestamp);

COMMIT;

--         The code used a BEGIN and COMMIT to ensure a single insert of the 5
--         rows.

-- 3.5.2   Check you have rows in Golfers, there should be 5.

SELECT * FROM golfers;


-- 3.5.3   Check the stream to see what happened.
--         Here you should only see rows with METADATA$ACTION = INSERT and
--         METADATA$UPDATE = FALSE

SELECT * FROM golfers_changes;


-- 3.5.4   Check the view.

SELECT * FROM Golfers_change_data ORDER BY 1;


-- 3.5.5   Now perform the merge statement.

MERGE INTO Golfers_history gh -- Target table to merge changes from GOLFERS
USING golfers_change_data m -- Golfers_change_data is a view that holds the logic that determines what to insert/update into the GOLFERS_HISTORY table.
   ON  gh.Golfer_ID = m.Golfer_ID -- Golfer_ID and start_time determine whether there is a unique record in the GOLFERS_HISTORY table
   AND gh.start_time = m.start_time
WHEN MATCHED AND m.dml_type = 'U' then
UPDATE -- Indicates the record has been updated and is no longer current and the end_time needs to be stamped
    SET gh.end_time = m.end_time,
        gh.current_flag = 0
WHEN MATCHED AND m.dml_type = 'D' then
UPDATE -- Deletes are essentially logical deletes. The record is stamped and no newer version is inserted
    SET gh.end_time = m.end_time,
        gh.current_flag = 0
WHEN NOT MATCHED and m.dml_type = 'I'
THEN INSERT -- Inserting a new Golfer_ID and updating an existing one both result in an insert
           (Golfer_ID  
           ,First_Name   
           ,Last_Name    
           ,Middle_Initial
           ,Date_Of_Birth
           ,Email_Address
           ,start_time
           ,end_time
           ,current_flag)
    VALUES (m.Golfer_ID
           ,m.First_Name
           ,m.Last_Name
           ,m.Middle_Initial
           ,m.Date_Of_Birth
           ,m.Email_Address
           ,m.start_time
           ,m.end_time
           ,m.current_flag);


-- 3.5.6   Let’s view the history table, there should be 5 new rows.

SELECT * FROM Golfers_History ORDER BY Golfer_Id;


-- 3.5.7   The stream has been consumed, so there should be no rows in there
--         now.

SELECT * FROM Golfers_Changes;


-- 3.5.8   Again, no rows here as the stream has been consumed.

SELECT * FROM Golfers_change_data;


-- 3.5.9   Now Let’s perform some data changes.
--         Give golfer_id = 1 a middle initial

SET update_timestamp = current_timestamp()::timestamp_ntz;

BEGIN;

UPDATE Golfers
    SET Middle_Initial = 'H' , Update_Timestamp = $update_timestamp
    WHERE Golfer_Id = 1;

COMMIT;


-- 3.5.10  Re-run the merge statement.

MERGE INTO Golfers_history gh
USING golfers_change_data m
   ON  gh.Golfer_ID = m.Golfer_ID
   AND gh.start_time = m.start_time
WHEN MATCHED AND m.dml_type = 'U' then
UPDATE
    SET gh.end_time = m.end_time,
        gh.current_flag = 0
WHEN MATCHED AND m.dml_type = 'D' then
UPDATE
    SET gh.end_time = m.end_time,
        gh.current_flag = 0
WHEN NOT MATCHED AND m.dml_type = 'I'
THEN INSERT
           (Golfer_ID  
           ,First_Name   
           ,Last_Name    
           ,Middle_Initial
           ,Date_Of_Birth
           ,Email_Address
           ,start_time
           ,end_time
           ,current_flag)
    VALUES (m.Golfer_ID
           ,m.First_Name
           ,m.Last_Name
           ,m.Middle_Initial
           ,m.Date_Of_Birth
           ,m.Email_Address
           ,m.start_time
           ,m.end_time
           ,m.current_flag);


-- 3.5.11  Now check the history table.
--         You should see two rows for the golfer_id = 1

SELECT * FROM Golfers_History order by Golfer_Id;


-- 3.5.12  Let’s delete a row in the Golfers Table.

DELETE FROM Golfers WHERE Golfer_id = 1;


-- 3.5.13  ReRun the merge.

MERGE INTO Golfers_history gh
USING golfers_change_data m
   ON  gh.Golfer_ID = m.Golfer_ID
   AND gh.start_time = m.start_time
WHEN MATCHED AND m.dml_type = 'U' then
UPDATE
    SET gh.end_time = m.end_time,
        gh.current_flag = 0
WHEN MATCHED AND m.dml_type = 'D' then
UPDATE
    SET gh.end_time = m.end_time,
        gh.current_flag = 0
WHEN NOT MATCHED AND m.dml_type = 'I'
THEN INSERT
           (Golfer_ID  
           ,First_Name   
           ,Last_Name    
           ,Middle_Initial
           ,Date_Of_Birth
           ,Email_Address
           ,start_time
           ,end_time
           ,current_flag)
    VALUES (m.Golfer_ID
           ,m.First_Name
           ,m.Last_Name
           ,m.Middle_Initial
           ,m.Date_Of_Birth
           ,m.Email_Address
           ,m.start_time
           ,m.end_time
           ,m.current_flag);


-- 3.5.14  View the golfers_history table.

SELECT * FROM Golfers_History ORDER BY Golfer_Id;


-- 3.5.15  ReInsert The golfer with the correct middle initial.

SET update_timestamp = current_timestamp()::timestamp_ntz;

BEGIN;

INSERT INTO Golfers VALUES
 (1,'Arnold','Palmer','D',to_date('1929-09-10','YYYY-MM-DD'), 'arnold.palmer@snowflakeuni.com',$update_timestamp);

COMMIT;


-- 3.5.16  ReRun the merge.

MERGE INTO Golfers_history gh
USING golfers_change_data m
   ON  gh.Golfer_ID = m.Golfer_ID
   AND gh.start_time = m.start_time
WHEN MATCHED AND m.dml_type = 'U' then
UPDATE
    SET gh.end_time = m.end_time,
        gh.current_flag = 0
WHEN MATCHED AND m.dml_type = 'D' then
UPDATE
    SET gh.end_time = m.end_time,
        gh.current_flag = 0
WHEN NOT MATCHED AND m.dml_type = 'I'
THEN INSERT
           (Golfer_ID  
           ,First_Name   
           ,Last_Name    
           ,Middle_Initial
           ,Date_Of_Birth
           ,Email_Address
           ,start_time
           ,end_time
           ,current_flag)
    VALUES (m.Golfer_ID
           ,m.First_Name
           ,m.Last_Name
           ,m.Middle_Initial
           ,m.Date_Of_Birth
           ,m.Email_Address
           ,m.start_time
           ,m.end_time
           ,m.current_flag);


-- 3.5.17  View the golfers_history table.

SELECT * FROM Golfers_History ORDER BY Golfer_Id;


-- 3.5.18  Clean up.

USE SCHEMA TAPIR_arch_db.public;
DROP SCHEMA TAPIR_arch_db.cdc_lab CASCADE;


-- 3.6.0   Key Takeaways
--         In this lab you have learned
--         - A Snowflake stream itself does not contain any table data. A stream
--         only stores an offset for the source object and returns CDC records
--         by leveraging the versioning history for the source object.
--         - You can use a stream to create a Slowly Changing Dimension Type 2,
--         which maintains the complete history of a record in a target.
