
-- 9.0.0   Row Level Security
--         By the end of this lab you will be able to:
--         - Understand how to implement row level security using Snowflake Row
--         Level policies
--         - Understand the usage of ROLES when implementing row level security
--         - Understand the usage of a mapping table to assist with row level
--         security
--         - Understand the implementation characteristics between the functions
--         current_role() verses current_available_roles() and when to use them
--         - Build and deploy row level policies using the taxation data
--         - How to test the row level policies using different roles
--         This Lab will show two implementations:
--         Using current_role()
--         Using current_available_roles()
--         What is the difference between these two functions / implementations?
--         - Current_Role() This function returns the role that is currently
--         active. It does not include any role inheritance, nor any additional
--         roles that a user has been granted. For basic row level access
--         policies, this works well but where more complex requirements exist,
--         this method becomes restrictive and tends to require more roles than
--         necessary. The Lab, will help show this.
--         - Current_Available_Roles() This function returns all of the roles a
--         user has been granted, either directly or through role inheritance.
--         For more complicated row-level access requirements, this method works
--         well and is flexible. Again this will be described in the exercises
--         below
--         To help best demonstrate these concepts, here are two requirements
--         The table that will be used for demonstrating this will be a TAXPAYER
--         table, this table contains many attributes. The two attributes that
--         will be used are US_State and TaxPayer_Type (Corporate or Individual)
--         A mapping table must be used to associate roles with US States.
--         - Requirement 1: Need to allow a user to only see data for the US
--         states(Northern or Southern) they are authorized to see
--         - Requirement 2: Need the ability of add an addition level of
--         filtering such that a user will only see authorized states and for a
--         given account type
--         The environment in which we will work, looks like this:
--         Its best practice to place all policies into a common database that
--         can be shared throughout various environments. This includes:
--         - Dynamic data mask policies
--         - Row level policies
--         - Tags

-- 9.1.0   Implementation 1: Current_Role()
--         This implementation will address Requirement 1 first and then attempt
--         to incorporate Requirement 2.

-- 9.1.1   Firstly, create an environment in which we can use.
--         To build this environment, use the following SQL

-- Setup
USE ROLE arch_role;
CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh;
USE WAREHOUSE TAPIR_arch_wh;

-- Central Policy DB
CREATE DATABASE IF NOT EXISTS  TAPIR_policy_db;
CREATE SCHEMA IF NOT EXISTS  TAPIR_policy_db.policies;

-- Tax DB
CREATE DATABASE IF NOT EXISTS  TAPIR_tax_db;
CREATE SCHEMA IF NOT EXISTS TAPIR_tax_db.taxation;

USE SCHEMA TAPIR_tax_db.taxation;
CREATE TABLE IF NOT EXISTS taxpayer CLONE training_tax_db.taxschema.taxpayer;
CREATE TABLE IF NOT EXISTS taxuser_mapping CLONE training_tax_db.taxschema.taxuser_mapping;

-- Policy Roles
CREATE ROLE IF NOT EXISTS TAPIR_north_states_role;
CREATE ROLE IF NOT EXISTS TAPIR_south_states_role;

-- RBAC for TAX_DB: TAPIR_north_states_role
-- We need a ROLE that is named according to the data it can access and have access to the database objects
GRANT USAGE ON DATABASE TAPIR_tax_db TO ROLE TAPIR_north_states_role;
GRANT USAGE ON SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_north_states_role;
GRANT USAGE ON WAREHOUSE TAPIR_arch_wh TO ROLE TAPIR_north_states_role;
GRANT SELECT ON ALL TABLES IN SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_north_states_role;
GRANT ROLE TAPIR_north_states_role TO USER TAPIR;

-- RBAC for TAX_DB: TAPIR_south_states_role
-- We need a ROLE that is named according to the data it can access and have access to the database objects
GRANT USAGE ON DATABASE TAPIR_tax_db TO ROLE TAPIR_south_states_role;
GRANT USAGE ON SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_south_states_role;
GRANT USAGE ON WAREHOUSE TAPIR_arch_wh TO ROLE TAPIR_south_states_role;
GRANT SELECT ON ALL TABLES IN SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_south_states_role;
GRANT ROLE TAPIR_south_states_role TO USER TAPIR;


-- 9.1.2   Reload the TAXUSER_MAPPING table.

-- Clear any existing data
TRUNCATE TABLE TAPIR_tax_db.taxation.taxuser_mapping;

-- Associate States with Roles for north and south
INSERT INTO TAPIR_tax_db.taxation.taxuser_mapping VALUES
 ('TAPIR_NORTH_STATES_ROLE', 'IL')
,('TAPIR_NORTH_STATES_ROLE', 'UT')
,('TAPIR_NORTH_STATES_ROLE', 'NV')
,('TAPIR_NORTH_STATES_ROLE', 'OR')
,('TAPIR_NORTH_STATES_ROLE', 'WV')
,('TAPIR_NORTH_STATES_ROLE', 'VT')
,('TAPIR_NORTH_STATES_ROLE', 'NJ')
,('TAPIR_NORTH_STATES_ROLE', 'NY')
,('TAPIR_NORTH_STATES_ROLE', 'WA')
,('TAPIR_NORTH_STATES_ROLE', 'IA')
,('TAPIR_NORTH_STATES_ROLE', 'OH')
,('TAPIR_NORTH_STATES_ROLE', 'ID')
,('TAPIR_NORTH_STATES_ROLE', 'ID')
,('TAPIR_SOUTH_STATES_ROLE', 'TX')
,('TAPIR_SOUTH_STATES_ROLE', 'CA')
,('TAPIR_SOUTH_STATES_ROLE', 'AZ')
,('TAPIR_SOUTH_STATES_ROLE', 'SC')
,('TAPIR_SOUTH_STATES_ROLE', 'FL')
,('TAPIR_SOUTH_STATES_ROLE', 'TN')
,('TAPIR_SOUTH_STATES_ROLE', 'AL')
,('TAPIR_SOUTH_STATES_ROLE', 'OK');

--         There are a few points to highlight
--         - The owner of the central policy database and TAPIR_TAX_DB is
--         arch_role.
--         - Two roles with access to database objects have been built that
--         match the requirement 1.

-- 9.1.3   Create a Row Level Access Policy.
--         The purpose of this access policy is to:
--         - Allow a users granted the TAPIR_NORTH_STATES_ROLE see only data
--         for US Northern States
--         - Allow a users granted the TAPIR_SOUTH_STATES_ROLE see only data
--         for US Southern States
--         - Disallow access to data if not using either of the above roles.

-- 9.1.4   The following SQL will create the row level access policy.

-- Arch_role has access to create the policies
USE ROLE arch_role;
USE SCHEMA TAPIR_policy_db.policies;

-- Create the policy
CREATE OR REPLACE ROW ACCESS POLICY taxdata_state_policy
   AS (taxpayerstate varchar(30)) returns Boolean ->
       EXISTS (
            SELECT 1 FROM TAPIR_tax_db.TAXATION.taxuser_mapping map
              WHERE map.taxuser_role = current_role()
              AND map.taxpayer_state= taxpayerstate
              )
;

-- Associate the policy to the table
ALTER TABLE TAPIR_tax_db.taxation.taxpayer
     ADD ROW ACCESS POLICY TAPIR_policy_db.policies.taxdata_state_policy ON (state);

--         Note: This policy should work fine providing the user has set the
--         ROLE to TAPIR_NORTH_STATES_ROLE or TAPIR_SOUTH_STATES_ROLE
--         By using the mapping table, we avoid the need to hard code any role
--         names in the policy.

-- 9.1.5   Let’s test this using the ROLE TAPIR_NORTH_STATES_ROLE.

USE ROLE TAPIR_north_states_role;
SELECT FirstName
      ,LastName
      ,City
      ,State
      ,TAXPAYER_TYPE
FROM TAPIR_tax_db.taxation.taxpayer;

--         The only rows returned are what has been categorized as the Northern
--         States:

-- 9.1.6   Let’s test this using the ROLE TAPIR_SOUTH_STATES_ROLE.

USE ROLE TAPIR_south_states_role;
SELECT firstname
      ,LastName
      ,City
      ,State
      ,TAXPAYER_TYPE
FROM TAPIR_tax_db.taxation.taxpayer;

--         The only rows returned are what has been categorized as the Southern
--         States:

-- 9.1.7   Let’s test this using the arch_role.

USE ROLE arch_role;
SELECT FirstName
      ,LastName
      ,City
      ,State
      ,TAXPAYER_TYPE
FROM TAPIR_tax_db.taxation.taxpayer;

--         Now we see no rows returned:
--         At this stage, all is well. Now lets introduce requirement 2, the
--         need to apply further filtration :
--         We would have to introduce four new roles, one for each permutation :
--         - TAPIR_SOUTH_CORPORATE_ROLE
--         - TAPIR_NORTH_CORPORATE_ROLE
--         - TAPIR_SOUTH_INDIVIDUAL_ROLE
--         - TAPIR_NORTH_INDIVIDUAL_ROLE
--         We will show a more simplified method later.
--         Each of these roles will require RBAC access to TAPIR_TAX_DB, we
--         also need to update the mapping tables.

-- 9.1.8   Run the following to perform RBAC for the above tasks.

-- Set context
USE ROLE arch_role;

-- RBAC for TAX_DB: TAPIR_south_corporate_role
CREATE ROLE TAPIR_south_corporate_role;
GRANT USAGE ON DATABASE TAPIR_tax_db TO ROLE TAPIR_south_corporate_role;
GRANT USAGE ON SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_south_corporate_role;
GRANT USAGE ON WAREHOUSE TAPIR_arch_wh TO ROLE TAPIR_south_corporate_role;
GRANT SELECT ON ALL TABLES IN SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_south_corporate_role;
GRANT ROLE TAPIR_south_corporate_role TO USER TAPIR;

-- RBAC for TAX_DB: TAPIR_north_corporate_role
CREATE ROLE TAPIR_north_corporate_role;
GRANT USAGE ON DATABASE TAPIR_tax_db TO ROLE TAPIR_north_corporate_role;
GRANT USAGE ON SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_north_corporate_role;
GRANT USAGE ON WAREHOUSE TAPIR_arch_wh TO ROLE TAPIR_north_corporate_role;
GRANT SELECT ON ALL TABLES IN SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_north_corporate_role;
GRANT ROLE TAPIR_north_corporate_role TO USER TAPIR;

-- RBAC for TAX_DB: TAPIR_south_individual_role
CREATE ROLE TAPIR_south_individual_role;
GRANT USAGE ON DATABASE TAPIR_tax_db TO ROLE TAPIR_south_individual_role;
GRANT USAGE ON SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_south_individual_role;
GRANT USAGE ON WAREHOUSE TAPIR_arch_wh TO ROLE TAPIR_south_individual_role;
GRANT SELECT ON ALL TABLES IN SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_south_individual_role;
GRANT ROLE TAPIR_south_individual_role TO USER TAPIR;

-- RBAC for  TAX_DB: TAPIR_north_individual_role
CREATE ROLE TAPIR_north_individual_role;
GRANT USAGE ON DATABASE TAPIR_tax_db TO ROLE TAPIR_north_individual_role;
GRANT USAGE ON SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_north_individual_role;
GRANT USAGE ON WAREHOUSE TAPIR_arch_wh TO ROLE TAPIR_north_individual_role;
GRANT SELECT ON ALL TABLES IN SCHEMA TAPIR_tax_db.taxation TO ROLE TAPIR_north_individual_role;
GRANT ROLE TAPIR_north_individual_role TO USER TAPIR;


-- 9.1.9   Run the following to update the mapping for the above tasks.

-- Context

USE ROLE arch_role;
USE SCHEMA TAPIR_tax_db.taxation;
CREATE TABLE IF NOT EXISTS tax_mapping CLONE training_tax_db.taxschema.tax_mapping;
INSERT INTO tax_mapping VALUES
 ('TAPIR_SOUTH_CORPORATE_ROLE','Corporate')
,('TAPIR_NORTH_CORPORATE_ROLE','Corporate')
,('TAPIR_SOUTH_INDIVIDUAL_ROLE','Individual')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','Individual');

-- Also Need to Add there roles into the TAXUSER_MAPPING table
INSERT INTO TAPIR_tax_db.taxation.taxuser_mapping VALUES
 ('TAPIR_NORTH_CORPORATE_ROLE', 'IL')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','IL')
,('TAPIR_NORTH_CORPORATE_ROLE', 'UT')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','UT')
,('TAPIR_NORTH_CORPORATE_ROLE', 'NV')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','NV')
,('TAPIR_NORTH_CORPORATE_ROLE', 'OR')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','OR')
,('TAPIR_NORTH_CORPORATE_ROLE', 'WV')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','WV')
,('TAPIR_NORTH_CORPORATE_ROLE', 'VT')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','VT')
,('TAPIR_NORTH_CORPORATE_ROLE', 'NJ')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','NJ')
,('TAPIR_NORTH_CORPORATE_ROLE', 'NY')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','NY')
,('TAPIR_NORTH_CORPORATE_ROLE', 'WA')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','WA')
,('TAPIR_NORTH_CORPORATE_ROLE', 'IA')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','IA')
,('TAPIR_NORTH_CORPORATE_ROLE', 'OH')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','OH')
,('TAPIR_NORTH_CORPORATE_ROLE', 'ID')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','ID')
,('TAPIR_NORTH_CORPORATE_ROLE', 'NY')
,('TAPIR_NORTH_INDIVIDUAL_ROLE','NY')
,('TAPIR_SOUTH_CORPORATE_ROLE', 'TX')
,('TAPIR_SOUTH_INDIVIDUAL_ROLE','TX')
,('TAPIR_SOUTH_CORPORATE_ROLE', 'CA')
,('TAPIR_SOUTH_INDIVIDUAL_ROLE','CA')
,('TAPIR_SOUTH_CORPORATE_ROLE', 'AZ')
,('TAPIR_SOUTH_INDIVIDUAL_ROLE','AZ')
,('TAPIR_SOUTH_CORPORATE_ROLE', 'SC')
,('TAPIR_SOUTH_INDIVIDUAL_ROLE','SC')
,('TAPIR_SOUTH_CORPORATE_ROLE', 'FL')
,('TAPIR_SOUTH_INDIVIDUAL_ROLE','FL')
,('TAPIR_SOUTH_CORPORATE_ROLE', 'AL')
,('TAPIR_SOUTH_INDIVIDUAL_ROLE','AL')
,('TAPIR_SOUTH_CORPORATE_ROLE', 'OK')
,('TAPIR_SOUTH_INDIVIDUAL_ROLE','OK')
,('TAPIR_SOUTH_CORPORATE_ROLE', 'TN')
,('TAPIR_SOUTH_INDIVIDUAL_ROLE','TN');


-- 9.1.10  Now let’s create the access policy.

USE ROLE arch_role;
USE SCHEMA TAPIR_policy_db.policies;

-- Remove the old access policy
ALTER TABLE TAPIR_tax_db.taxation.taxpayer DROP ROW ACCESS POLICY taxdata_state_policy;
DROP ROW ACCESS POLICY TAPIR_policy_db.policies.taxdata_state_policy;

-- Create the new One
CREATE or REPLACE ROW ACCESS POLICY taxdata_access_policy
   AS (taxpayertype varchar(30), taxpayerstate varchar(30)) returns Boolean ->
       EXISTS (
            SELECT 1 FROM TAPIR_tax_db.taxation.tax_mapping map
              WHERE map.taxuser_role = current_role()
              AND map.taxpayer_type=taxpayertype
              )
       AND
       EXISTS (
            SELECT 1 FROM TAPIR_tax_db.taxation.taxuser_mapping map
              WHERE map.taxuser_role = current_role()
              AND map.taxpayer_state=taxpayerstate
              )
;


-- Associate the policy to the table
ALTER TABLE TAPIR_tax_db.taxation.taxpayer
     ADD ROW ACCESS POLICY taxdata_access_policy ON (taxpayer_type,state);


-- 9.1.11  Let’s test role TAPIR_SOUTH_INDIVIDUAL_ROLE.

USE ROLE TAPIR_south_individual_role;
SELECT FirstName
      ,LastName
      ,City
      ,State
      ,TAXPAYER_TYPE
FROM TAPIR_tax_db.taxation.taxpayer;

--         The result should show only Southern State individuals.

-- 9.2.0   Implementation 2: Current_Available_Roles()
--         Now, we shall implement the same requirements as above but using the
--         function - current_available_roles() For this, we introduce the
--         concept of Secure Data Roles (SDR), these roles should be prefixed
--         _SDR.
--         The key thing about a Secure Data Role, is it purely gives the end
--         user access to sensitive data. An SDR role has no actual access to
--         the underlying tables. It simply used an as indicator for sensitive
--         data access.

-- 9.2.1   Starting from scratch, lets first clean up what was performed with
--         implementation 1.

-- Tidy Up
USE ROLE arch_role;
DROP DATABASE TAPIR_tax_db CASCADE;
DROP DATABASE TAPIR_policy_db CASCADE;
DROP ROLE  TAPIR_north_states_role;
DROP ROLE  TAPIR_south_states_role;
DROP ROLE TAPIR_south_corporate_role;
DROP ROLE TAPIR_north_corporate_role;
DROP ROLE TAPIR_south_individual_role;
DROP ROLE TAPIR_north_individual_role;


-- 9.2.2   Create the objects required for both scenarios, including the _SDR
--         Roles.

-- Perform the set up
USE ROLE arch_role;
USE WAREHOUSE TAPIR_arch_wh;

-- Central Policy DB
CREATE DATABASE IF NOT EXISTS  TAPIR_policy_db;
CREATE SCHEMA IF NOT EXISTS  TAPIR_policy_db.policies;

-- Tax DB
CREATE DATABASE IF NOT EXISTS  TAPIR_tax_db;
CREATE SCHEMA IF NOT EXISTS TAPIR_tax_db.taxation;

USE SCHEMA TAPIR_tax_db.taxation;
CREATE TABLE IF NOT EXISTS taxpayer CLONE training_tax_db.taxschema.taxpayer;
CREATE TABLE IF NOT EXISTS taxuser_mapping CLONE training_tax_db.taxschema.taxuser_mapping;
CREATE TABLE IF NOT EXISTS tax_mapping CLONE training_tax_db.taxschema.tax_mapping;

-- Create policy Roles (Scenario 1)
CREATE ROLE IF NOT EXISTS _SDR_TAPIR_north_states_role;
CREATE ROLE IF NOT EXISTS _SDR_TAPIR_south_states_role;

-- Create policy Roles (Scenario 2)
CREATE ROLE IF NOT EXISTS _SDR_TAPIR_south_corporate_role;
CREATE ROLE IF NOT EXISTS _SDR_TAPIR_north_corporate_role;
CREATE ROLE IF NOT EXISTS _SDR_TAPIR_south_individual_role;
CREATE ROLE IF NOT EXISTS _SDR_TAPIR_north_individual_role;

--         The above only has to grant the _SDR roles, notice no RBAC privileges
--         controls are applied to the _SDR role.
--         We still need to create the metadata for this.

-- 9.2.3   Run the following to perform this task.

-- Set context
USE ROLE arch_role;
USE SCHEMA TAPIR_tax_db.taxation;

-- Clear any existing data
TRUNCATE TABLE taxuser_mapping;
TRUNCATE TABLE tax_mapping;


-- Associate States with Roles for north and south
INSERT INTO taxuser_mapping VALUES
 ('_SDR_TAPIR_NORTH_STATES_ROLE', 'IL')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'UT')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'NV')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'OR')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'WV')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'VT')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'NJ')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'NY')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'WA')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'IA')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'OH')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'ID')
,('_SDR_TAPIR_NORTH_STATES_ROLE', 'ID')
,('_SDR_TAPIR_SOUTH_STATES_ROLE', 'TX')
,('_SDR_TAPIR_SOUTH_STATES_ROLE', 'CA')
,('_SDR_TAPIR_SOUTH_STATES_ROLE', 'AZ')
,('_SDR_TAPIR_SOUTH_STATES_ROLE', 'SC')
,('_SDR_TAPIR_SOUTH_STATES_ROLE', 'FL')
,('_SDR_TAPIR_SOUTH_STATES_ROLE', 'TN')
,('_SDR_TAPIR_SOUTH_STATES_ROLE', 'AL')
,('_SDR_TAPIR_SOUTH_STATES_ROLE', 'OK');

INSERT INTO tax_mapping VALUES
 ('_SDR_TAPIR_SOUTH_CORPORATE_ROLE','Corporate')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE','Corporate')
,('_SDR_TAPIR_SOUTH_INDIVIDUAL_ROLE','Individual')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','Individual');

-- Also Need to Add there roles into the TAXUSER_MAPPING table
INSERT INTO TAPIR_tax_db.taxation.taxuser_mapping VALUES
 ('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'IL')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','IL')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'UT')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','UT')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'NV')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','NV')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'OR')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','OR')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'WV')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','WV')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'VT')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','VT')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'NJ')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','NJ')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'NY')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','NY')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'WA')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','WA')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'IA')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','IA')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'OH')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','OH')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'ID')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','ID')
,('_SDR_TAPIR_NORTH_CORPORATE_ROLE', 'NY')
,('_SDR_TAPIR_NORTH_INDIVIDUAL_ROLE','NY')
,('_SDR_TAPIR_SOUTH_CORPORATE_ROLE', 'TX')
,('_SDR_TAPIR_SOUTH_INDIVIDUAL_ROLE','TX')
,('_SDR_TAPIR_SOUTH_CORPORATE_ROLE', 'CA')
,('_SDR_TAPIR_SOUTH_INDIVIDUAL_ROLE','CA')
,('_SDR_TAPIR_SOUTH_CORPORATE_ROLE', 'AZ')
,('_SDR_TAPIR_SOUTH_INDIVIDUAL_ROLE','AZ')
,('_SDR_TAPIR_SOUTH_CORPORATE_ROLE', 'SC')
,('_SDR_TAPIR_SOUTH_INDIVIDUAL_ROLE','SC')
,('_SDR_TAPIR_SOUTH_CORPORATE_ROLE', 'FL')
,('_SDR_TAPIR_SOUTH_INDIVIDUAL_ROLE','FL')
,('_SDR_TAPIR_SOUTH_CORPORATE_ROLE', 'AL')
,('_SDR_TAPIR_SOUTH_INDIVIDUAL_ROLE','AL')
,('_SDR_TAPIR_SOUTH_CORPORATE_ROLE', 'OK')
,('_SDR_TAPIR_SOUTH_INDIVIDUAL_ROLE','OK')
,('_SDR_TAPIR_SOUTH_CORPORATE_ROLE', 'TN')
,('_SDR_TAPIR_SOUTH_INDIVIDUAL_ROLE','TN');


-- 9.2.4   Now, lets create the row level policy to implement scenario 2.

USE SCHEMA TAPIR_policy_db.policies;

CREATE or REPLACE ROW ACCESS POLICY taxdata_access_policy
   AS (taxpayertype varchar(30), taxpayerstate varchar(30)) returns Boolean ->
       EXISTS (
            SELECT 1 FROM TAPIR_tax_db.taxation.tax_mapping map
              WHERE contains(current_available_roles(), map.taxuser_role)
              AND map.taxpayer_type=taxpayertype
              )
       AND
       EXISTS (
            SELECT 1 FROM TAPIR_tax_db.taxation.taxuser_mapping map
              WHERE contains(current_available_roles(), map.taxuser_role)
              AND map.taxpayer_state=taxpayerstate
              )
;

ALTER TABLE TAPIR_tax_db.taxation.taxpayer
     ADD ROW ACCESS POLICY Taxdata_access_policy ON (taxpayer_type,state);


-- 9.2.5   At the moment selecting from table TAPIR_tax_db.TAXATION.TAXPAYER
--         should return no rows.
--         Let’s be sure thats the case

USE ROLE arch_role;
SELECT FirstName
      ,LastName
      ,City
      ,State
      ,TAXPAYER_TYPE
FROM TAPIR_TAX_DB.TAXATION.TAXPAYER;

--         Note: Role Arch_role is prevented from seeing the data because of the
--         POLICY enforced.

-- 9.2.6   If we now grant the _SDR_SOUTH_CORPORATE_ROLE role to user TAPIR,
--         they can now see the associated sensitive data.
--         Lets test that

USE ROLE arch_role;
GRANT ROLE _sdr_TAPIR_south_corporate_role TO USER TAPIR;
SELECT FirstName
      ,LastName
      ,City
      ,State
      ,TAXPAYER_TYPE
FROM TAPIR_tax_db.taxation.taxpayer;

--         We now see only the southern corporate data in TAX_PAYER.

-- 9.3.0   Tidy Up
--         Let’s clean up the objects and roles we created. The following sql
--         will perform this task:

USE ROLE arch_role;
DROP DATABASE TAPIR_tax_db CASCADE;
DROP DATABASE TAPIR_policy_db CASCADE;
DROP ROLE IF EXISTS TAPIR_north_states_role;
DROP ROLE IF EXISTS TAPIR_south_states_role;
DROP ROLE IF EXISTS TAPIR_south_corporate_role;
DROP ROLE IF EXISTS TAPIR_north_corporate_role;
DROP ROLE IF EXISTS TAPIR_south_individual_role;
DROP ROLE IF EXISTS TAPIR_north_individual_role;
DROP ROLE IF EXISTS _SDR_TAPIR_north_states_role;
DROP ROLE IF EXISTS _SDR_TAPIR_south_states_role;
DROP ROLE IF EXISTS _SDR_TAPIR_south_corporate_role;
DROP ROLE IF EXISTS _SDR_TAPIR_north_corporate_role;
DROP ROLE IF EXISTS _SDR_TAPIR_south_individual_role;
DROP ROLE IF EXISTS _SDR_TAPIR_north_individual_role;


-- 9.4.0   Key Takeaways
--         You should now understand how to apply row level security in
--         Snowflake.
--         - Knowledge of how to define, build and implement row level security
--         within Snowflake.
--         - Knowledge of how to define mapping tables for row level security.
--         - Understand the Best practices around where to store and maintain
--         policies
