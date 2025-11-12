
-- 5.0.0   Object Tagging
--         By the end of this lab, you will be able to:
--         - Create tags and apply them to a database, schema, table, and column
--         - Review Tags using SHOW commands
--         - Tag a warehouse
--         - Create a Stored Procedure to Operate on the warehouse Tag Values
--         TAGS are a flexible mechanism by which you can group and mark objects
--         for purposes such as:
--         - Aggregating usage and cost
--         - Denoting tables and columns as containing sensitive data
--         Tags can be applied to many different objects within Snowflake.

-- 5.1.0   Create a Schema and Set Context

-- 5.1.1   Create Worksheet from SQL File and create schema and set your
--         context.

USE ROLE arch_role;

CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh
 AUTO_SUSPEND = 60
 INITIALLY_SUSPENDED = true;

ALTER WAREHOUSE TAPIR_arch_wh SET WAREHOUSE_SIZE = medium;

USE WAREHOUSE TAPIR_arch_wh;
CREATE DATABASE IF NOT EXISTS TAPIR_arch_db;
USE DATABASE TAPIR_arch_db;
CREATE SCHEMA IF NOT EXISTS TAPIR_arch_db.governance;
USE SCHEMA TAPIR_arch_db.governance;



-- 5.2.0   Object Tagging Lab Section
--         The following few steps introduce the use of object tagging in
--         Snowflake:
--         STEP 1: Create a table to tag
--         STEP 2: Create tags
--         STEP 3: Apply tags to a database, schema, table, and column
--         STEP 4: Review the tags

-- 5.2.1   STEP 1: Create a table to tag.
--         As a first step, create a table to which tags will be applied shortly
--         and populate it with initial values.
--         Execute the following:

-- Create table
CREATE OR REPLACE TABLE CUSTOMERS (
    id INTEGER AUTOINCREMENT (1,1),
    cust_number VARCHAR(10),
    first_name VARCHAR(100),
    last_name VARCHAR(100)
);

-- Populate the table with initial values
INSERT INTO customers(cust_number, first_name, last_name) VALUES
        ('A1234', 'Adam','Adler'),
        ('B2345', 'Benny','Bartoli'),
        ('C3456', 'Catherine','Carson'),
        ('D4567', 'Dougal','Davies'),
        ('E5678', 'Eleanor','Evans'),
        ('F6789', 'Fiona','Fearon'),
        ('G7891', 'Gary','Grayson'),
        ('H8911', 'Henrietta','Hewson'),
        ('I9112', 'Igor','Iverson'),
        ('J1123', 'Jenny','Juris');



-- 5.2.2   STEP 2: Create tags.
--         Now create tags for four data objects (database, schema, table,
--         column) that will be applied in the next step:

CREATE OR REPLACE TAG TAPIR_database_tag  COMMENT = 'TAPIR TAG FOR DATABASE';
CREATE OR REPLACE TAG TAPIR_schema_tag    COMMENT = 'TAPIR TAG FOR SCHEMA';
CREATE OR REPLACE TAG TAPIR_table_tag     COMMENT = 'TAPIR TAG FOR TABLE';
CREATE OR REPLACE TAG TAPIR_column_tag    COMMENT = 'TAPIR TAG FOR COLUMN';

--         Note that we can also create tags on account-level objects (user), as
--         per the following example:

CREATE OR REPLACE TAG TAPIR_user_tag      COMMENT = 'TAG FOR USER CREATED BY TAPIR';

--         In the example above, we created tags in the
--         TAPIR_arch_db.governance database and schema. This is an example of
--         decentralization from an administration perspective, as the tags are
--         located in the database in which they are used.
--         In practice, it may be preferable to centrally governed tags, with a
--         dedicated tag administrator managing these objects from a central tag
--         database and schema. E.g., the database and schema central.admin_tags
--         could be created to hold tags to be used across an entire Snowflake
--         instance.

-- 5.2.3   STEP 3: Apply tags to a database, schema, table, and column.
--         Now apply the tags to the objects that have been created. For these
--         initial examples, we’re using the fully qualified object name for a
--         tag value rather than something more meaningful that may be used in
--         the real world. Execute the following commands to do so:

ALTER DATABASE TAPIR_arch_db
  SET TAG TAPIR_DATABASE_TAG = 'TAPIR_arch_db';

ALTER SCHEMA TAPIR_arch_db.governance
  SET TAG TAPIR_SCHEMA_TAG = 'TAPIR_arch_db.governance';

ALTER TABLE TAPIR_arch_db.governance.customers
  SET TAG TAPIR_TABLE_TAG = 'TAPIR_arch_db.governance.customers';

ALTER TABLE TAPIR_arch_db.governance.customers
  MODIFY COLUMN first_name
  SET TAG TAPIR_COLUMN_TAG = 'TAPIR_arch_db.governance.customers.first_name';        


-- 5.2.4   STEP 4: Review the tags.
--         Let’s review the previous steps completed (create a table, create
--         tags, apply tags).
--         Snowflake provides several different means by which to track your
--         tags. The SHOW TAG command has a downward view from the object level
--         context it is run from. E.g., SHOW TAGS IN DATABASE will show all
--         tags at the database level and all other tags associated with objects
--         in that database. In the following output, you will see several
--         example tags created and applied by the Education Services team ahead
--         of time in the NEWFEATURES_DB database you cloned from.

SHOW TAGS IN ACCOUNT;
SHOW TAGS IN DATABASE TAPIR_arch_db;
SHOW TAGS IN SCHEMA   TAPIR_arch_db.governance;

--         The following Snowflake table functions also provide information on
--         tags.
--         TAG_REFERENCES returns a table in which each row displays an
--         association between a tag and value. The associated tag and value are
--         the result of a direct association with an object or through tag
--         lineage:

SELECT *
FROM TABLE(TAPIR_arch_db.information_schema.tag_references
  ('TAPIR_arch_db', 'database'));

SELECT *
FROM TABLE(TAPIR_arch_db.information_schema.tag_references
  ('TAPIR_arch_db.governance', 'schema'));


-- 5.2.5   Tag Inheritance.
--         A tag is inherited based on the Snowflake securable object hierarchy.
--         Snowflake recommends defining the tag keys as closely as possible to
--         the securable object in the hierarchy in your Snowflake environment.
--         Tag inheritance means that if a tag is applied to a table, the tag
--         also applies to the columns in that table. This behavior is referred
--         to as tag lineage. It is possible to override an inherited tag.
--         Use the table function to check the tags on the customers table and
--         the id column. Note that there is no SHOW command for table tags.

SELECT *
FROM
TABLE(TAPIR_arch_db.information_schema.tag_references
  ('TAPIR_arch_db.governance.customers', 'table'));

-- The following query will display the TAG REFERENCES in container order for the
-- column and up references, the second query for the table and up references:

SELECT *,
   CASE
      WHEN lower(level) = 'column' THEN 1
      WHEN lower(level) = 'table' THEN 2
      WHEN lower(level) = 'schema' THEN 3
      WHEN lower(level) = 'database' THEN 4
      ELSE 5
    END AS level_ord
FROM
   TABLE(TAPIR_arch_db.information_schema.tag_references
   ('TAPIR_arch_db.governance.customers.first_name', 'column'))
ORDER BY level_ord ASC;

SELECT *,
   CASE
      WHEN lower(level) = 'column' THEN 1
      WHEN lower(level) = 'table' THEN 2
      WHEN lower(level) = 'schema' THEN 3
      WHEN lower(level) = 'database' THEN 4
      ELSE 5
    END AS level_ord
FROM
   TABLE(TAPIR_arch_db.information_schema.tag_references
   ('TAPIR_arch_db.governance.customers.id', 'column'))
ORDER BY level_ord ASC;   

--         Look at the last query above result, which shows tags on the
--         CUSTOMERS.ID column. The LEVEL in the query result output shows tags
--         at the database, schema, and table levels. Although the ID column has
--         no column level tag, it still inherits the higher level tags. This
--         demonstrates tag inheritance, in this case, with database -> schema
--         -> table -> column.

-- 5.2.6   Review the Account Usage.
--         In addition to the INFORMATION_SCHEMA options, Snowflake also
--         provides objects in the ACCOUNT_USAGE schema, which catalog tag
--         details. Be aware that, as with all ACCOUNT_USAGE views and
--         functions, there is a latency in this data population. Please refer
--         to the Snowflake documentation for specifics.
--         Both objects (view and table function) in the following examples have
--         a latency of up to two hours. So you may not see your newly created
--         tags immediately, only those created more than two hours ago. To
--         ensure there is tag_reference data to view, tags have been created
--         and applied by the Education Services team.
--         Use this query to identify the associations between objects and tags:

SELECT *
FROM  
snowflake.account_usage.tag_references
ORDER BY tag_name, domain, object_id;

--         The following table function produces rows that display an
--         association between the specified tag and the Snowflake object with
--         which the tag is associated. Note that the object identifier needs to
--         be uppercase.

SELECT *
FROM
TABLE(snowflake.account_usage.tag_references_with_lineage
  ('NEWFEATURES_DB.GOVERNANCE.EXAMPLE_TABLE_TAG'));


-- 5.2.7   Object Tagging Example Scenario.
--         In the following example, we will create a tag assigned to the
--         warehouses, TAPIR_arch_wh and TAPIR_arch_wh2. The tag will have
--         two values: morning and afternoon. We will create a stored procedure
--         to analyze the value of the tag assigned to these warehouses, using
--         the system function SYSTEM$TAG. The appropriate virtual warehouse
--         will then be set as the virtual warehouse context for your session.

-- 5.2.8   Create a New Warehouse.
--         Execute the following statement:

CREATE OR REPLACE WAREHOUSE TAPIR_arch_wh2
  WAREHOUSE_SIZE=xsmall  
  INITIALLY_SUSPENDED = true;

--         Create the tag warehouse_tag that includes a list of allowed values.
--         We are predefining a list of values that will be enforced; when
--         setting the tag on the object, only one of the list of values can be
--         used.
--         Assign this tag to each virtual warehouse as follows:

CREATE OR REPLACE TAG warehouse_tag
  ALLOWED_VALUES 'morning','afternoon';

ALTER WAREHOUSE TAPIR_arch_wh
  SET TAG warehouse_tag = 'morning';

ALTER WAREHOUSE TAPIR_arch_wh2  
  SET TAG warehouse_tag = 'afternoon';


-- 5.2.9   Create a Stored Procedure to Operate on the Tag Values.
--         The following stored procedure considers the time of day (morning,
--         afternoon…) it is being executed. Based upon the interrogation of the
--         tag applied to the TAPIR_arch_wh and TAPIR_arch_wh2 warehouses,
--         it will choose the appropriate virtual warehouse to set as the
--         virtual warehouse context for the session.
--         In a real-world environment, we may want a separate virtual warehouse
--         (of a specific size and configuration) to be used by analysts during
--         the morning - perhaps when there is more activity - and another, with
--         smaller specifications, in the afternoon. The following example
--         models this type of approach.
--         Execute the following to create the stored procedure:

CREATE OR REPLACE PROCEDURE which_warehouse_by_time(day_segment STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    LET wh1_tag_value := (select system$get_tag('warehouse_tag', 'TAPIR_arch_wh', 'warehouse'));
    LET wh2_tag_value := (select system$get_tag('warehouse_tag', 'TAPIR_arch_wh2', 'warehouse'));

    CASE (day_segment)
    WHEN ('morning') THEN
        IF (wh1_tag_value = 'morning') THEN
            USE WAREHOUSE TAPIR_arch_wh;
        ELSEIF (wh2_tag_value = 'morning') THEN
            USE WAREHOUSE TAPIR_arch_wh2;
        END IF;
    WHEN ('afternoon') THEN
        IF (wh1_tag_value = 'afternoon') THEN
            use warehouse TAPIR_arch_wh;
        ELSEIF (wh2_tag_value = 'afternoon') THEN
            USE WAREHOUSE TAPIR_arch_wh2;
        END IF;
    END;
    RETURN 'Now using virtual warehouse ' || current_warehouse() || ' for the ' || day_segment || ' shift';
END;
$$;

--         Call the stored procedure, passing in a value to simulate the part of
--         the day. In the stored procedure, the virtual warehouse tag values
--         will be checked and matched with the part of the day passed in
--         day_segment. The stored procedure will set the virtual warehouse in
--         our session context and pass back confirmation of this.
--         Note: The Snowsight User Interface might not update showing the new
--         current warehouse.

-- simulate different parts of the day
CALL which_warehouse_by_time('morning');
SELECT current_warehouse();

CALL which_warehouse_by_time('afternoon');
SELECT current_warehouse();

--         You might like to experiment by swapping the tag values around for
--         the warehouses before running the stored procedure again:

ALTER WAREHOUSE TAPIR_arch_wh2  
  SET TAG warehouse_tag = 'morning';

ALTER WAREHOUSE TAPIR_arch_wh    
  SET TAG warehouse_tag = 'afternoon';

CALL which_warehouse_by_time('morning');
SELECT current_warehouse();

CALL which_warehouse_by_time('afternoon');
SELECT current_warehouse();


-- 5.2.10  Tidy up the lab.

-- Drop the Objects
DROP SCHEMA TAPIR_arch_db.governance;

-- Drop extra WAREHOUSE
DROP WAREHOUSE TAPIR_arch_wh2;

-- Suspend and Resize your warehouse.
ALTER WAREHOUSE TAPIR_arch_wh SUSPEND;
ALTER WAREHOUSE TAPIR_arch_wh SET
  WAREHOUSE_SIZE = XSMALL;


-- 5.3.0   Key Takeaways
--         In this lab you have learned
--         - Tags can be applied to many objects within Snowflake and used to
--         group or mark them for purposes like aggregating usage and cost or
--         denoting sensitive data.
--         - Classification utilizes object tags to label the data, which can
--         then be used to analyze and comply with privacy regulations.
