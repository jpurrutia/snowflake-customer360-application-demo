
-- 10.0.0  Environment Cloning
--         By the end of this lab you will be able to:
--         - Build an environment with the DB Roles Model
--         - Build a new environment using cloning
--         - Use show commands to examine grants on objects
--         - Determine a plan to correct the grants
--         - Write your own SQL code to the correct the grants

-- 10.1.0  Building the environment
--         To fully understand the environment privileges, its good to examine
--         and an environment from two points of view.
--         The Object Hierarchy
--         The Role Hierarchy

-- 10.1.1  View the Object Hierarchy.
--         The object hierarchy that we will be using in this lab is shown
--         below.
--         This is a relatively simple structure but is enough to show the key
--         principles of environment building using clones.

-- 10.1.2  View the Role Hierarchy.
--         The following diagram show the environment that will be built. This
--         environment is built using the RBAC model using database roles.
--         The orange boxes show the built in roles, the green boxes show
--         account level roles and the blue boxes show the database roles.
--         Some key principles:
--         Privileges against the schemas are granted to the database roles
--         (SCH1_RW, SCH1_RW, SCH2_RW and SCH1_RO)
--         The database roles are in turn granted to the account level
--         functional roles
--         New database role called DB_SYSADMIN is created. This database role
--         will be granted the ownership privilege of the schemas. This is an
--         important step for when the environment is to be cloned.

-- 10.1.3  Run the following DDL To build the environment.

-- Create environment admin roles
USE ROLE arch_role;
CREATE OR REPLACE ROLE TAPIR_PROD_sysadmin;
CREATE OR REPLACE ROLE TAPIR_PROD_secadmin;

-- Grant privileges to environment roles
GRANT CREATE DATABASE ON ACCOUNT TO ROLE TAPIR_PROD_sysadmin;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE TAPIR_PROD_sysadmin;

GRANT CREATE ROLE ON ACCOUNT TO TAPIR_PROD_secadmin;

-- Grant environment roles to your login

GRANT ROLE TAPIR_PROD_sysadmin TO USER TAPIR;
GRANT ROLE TAPIR_PROD_secadmin TO USER TAPIR;

-- Build DB Objects
USE ROLE TAPIR_PROD_sysadmin;
CREATE OR REPLACE DATABASE TAPIR_PROD;

-- DB Role to own the schemas, this helps when cloning
CREATE DATABASE ROLE TAPIR_PROD.db_sysadmin;
GRANT CREATE SCHEMA ON DATABASE TAPIR_prod TO DATABASE ROLE TAPIR_PROD.db_sysadmin;

GRANT DATABASE ROLE TAPIR_PROD.db_sysadmin TO ROLE TAPIR_PROD_sysadmin;
USE ROLE TAPIR_PROD_sysadmin;
USE DATABASE TAPIR_prod;

CREATE SCHEMA sch1 WITH MANAGED ACCESS;
CREATE SCHEMA sch2 WITH MANAGED ACCESS;

-- transfer ownership of schema to the database admin role
GRANT OWNERSHIP ON SCHEMA sch1 TO DATABASE ROLE db_sysadmin;
GRANT OWNERSHIP ON SCHEMA sch2 TO DATABASE ROLE db_sysadmin;

CREATE TABLE sch1.tab1 (id int, desc varchar(10));
CREATE TABLE sch2.tab1 (id int, desc varchar(10));
CREATE WAREHOUSE TAPIR_PROD_wh2 WAREHOUSE_SIZE = xsmall;

GRANT OWNERSHIP ON ALL TABLES IN DATABASE TAPIR_prod TO DATABASE ROLE db_sysadmin;

-- Create account level functional roles
USE ROLE TAPIR_PROD_secadmin;
CREATE ROLE TAPIR_PROD_Func_A;
CREATE ROLE TAPIR_PROD_Func_B;

-- Create the database access roles

USE ROLE TAPIR_PROD_sysadmin;
CREATE DATABASE ROLE sch1_RW;
CREATE DATABASE ROLE sch1_RO;
CREATE DATABASE ROLE sch2_RW;
CREATE DATABASE ROLE sch2_RO;

-- grant DB Usage priviledes
GRANT USAGE ON DATABASE TAPIR_PROD TO DATABASE ROLE sch1_RW;
GRANT USAGE ON DATABASE TAPIR_PROD TO DATABASE ROLE sch1_RO;
GRANT USAGE ON DATABASE TAPIR_PROD TO DATABASE ROLE sch2_RW;
GRANT USAGE ON DATABASE TAPIR_PROD TO DATABASE ROLE sch2_RO;

-- grant schema usage
GRANT USAGE ON SCHEMA TAPIR_PROD.sch1 TO DATABASE ROLE sch1_RW;
GRANT USAGE ON SCHEMA TAPIR_PROD.sch1 TO DATABASE ROLE sch1_RO;
GRANT USAGE ON SCHEMA TAPIR_PROD.sch2 TO DATABASE ROLE sch2_RW;
GRANT USAGE ON SCHEMA TAPIR_PROD.sch2 TO DATABASE ROLE sch2_RO;

-- Warehouse usage
GRANT USAGE ON WAREHOUSE TAPIR_PROD_wh2 TO ROLE TAPIR_PROD_Func_A;
GRANT USAGE ON WAREHOUSE TAPIR_PROD_wh2 TO ROLE TAPIR_PROD_Func_B;

-- grant object privileges
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA TAPIR_PROD.sch1 TO DATABASE ROLE sch1_RW;
GRANT SELECT ON ALL TABLES IN SCHEMA TAPIR_PROD.sch1 TO DATABASE ROLE sch1_RO;

GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA TAPIR_PROD.sch2 TO DATABASE ROLE sch2_RW;
GRANT SELECT ON ALL TABLES IN SCHEMA TAPIR_PROD.sch2 TO DATABASE ROLE sch2_RO;

-- Grant database roles to account level functional roles
GRANT DATABASE ROLE sch1_RW TO ROLE TAPIR_PROD_Func_A;
GRANT DATABASE ROLE sch2_RW TO ROLE TAPIR_PROD_Func_A;
GRANT DATABASE ROLE sch1_RO TO ROLE TAPIR_PROD_Func_B;
GRANT DATABASE ROLE sch2_RO TO ROLE TAPIR_PROD_Func_B;

-- Grant Functional Roles to our login
USE ROLE TAPIR_PROD_secadmin;
GRANT ROLE TAPIR_PROD_Func_A TO USER TAPIR;
GRANT ROLE TAPIR_PROD_Func_B TO USER TAPIR;

-- Add some basic data
USE ROLE TAPIR_PROD_Func_A;
USE WAREHOUSE TAPIR_PROD_wh2;
INSERT INTO TAPIR_PROD.sch1.tab1 VALUES (1,'AAA'),(2,'BBB'),(3,'CCC');
INSERT INTO TAPIR_PROD.sch2.tab1 VALUES (100,'AAAAA'),(200,'BBBBB'),(300,'CCCCC');

--         The environment is now built.

-- 10.2.0  Clone the Environment
--         There are a few steps to this:
--         Build the account level roles for the environment and functional
--         roles
--         Clone the environment
--         Grant the database access roles to the account level functional roles
--         Revoke the manage grants on account privilege from the environment
--         role

-- 10.2.1  First build the account level roles.
--         We will build a DEV environment from our production environment.
--         Before cloning the environment, we need to build the required
--         environment role hierarchy for the DEV environment.
--         Here the DEV environment roles and functional roles will be built.
--         The following SQL will create this:

USE ROLE arch_role;
CREATE ROLE TAPIR_DEV_sysadmin;
CREATE ROLE TAPIR_DEV_secadmin;

-- Grant privileges to environment roles
GRANT CREATE DATABASE ON ACCOUNT TO ROLE TAPIR_DEV_sysadmin;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE TAPIR_DEV_sysadmin;

GRANT MANAGE GRANTS ON ACCOUNT TO ROLE TAPIR_DEV_sysadmin;

GRANT CREATE ROLE ON ACCOUNT TO TAPIR_DEV_secadmin;

-- Grant environment roles to your login
GRANT ROLE TAPIR_DEV_sysadmin to user TAPIR;
GRANT ROLE TAPIR_DEV_secadmin to user TAPIR;

-- create the DEV environment account level functional roles
CREATE OR REPLACE ROLE TAPIR_DEV_Func_A;
CREATE OR REPLACE ROLE TAPIR_DEV_Func_B;

-- grant the DEV functional role to my login
GRANT ROLE TAPIR_DEV_Func_A TO USER TAPIR;
GRANT ROLE TAPIR_DEV_Func_B TO USER TAPIR;


-- 10.2.2  Next clone the environment.
--         Execute the following SQL to perform the clone.

-- Temporary grants
USE ROLE TAPIR_PROD_sysadmin;
GRANT USAGE ON DATABASE TAPIR_PROD TO ROLE TAPIR_DEV_sysadmin WITH GRANT OPTION;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TAPIR_PROD TO ROLE TAPIR_DEV_sysadmin WITH GRANT OPTION;
GRANT INSERT,UPDATE,DELETE,SELECT ON ALL TABLES IN DATABASE TAPIR_PROD TO ROLE TAPIR_DEV_sysadmin WITH GRANT OPTION;

-- Perform the clone
USE ROLE TAPIR_dev_sysadmin;

CREATE DATABASE TAPIR_DEV clone TAPIR_PROD;

-- create warehouse
CREATE WAREHOUSE TAPIR_DEV_wh1 WAREHOUSE_SIZE = xsmall;
GRANT USAGE ON WAREHOUSE TAPIR_DEV_wh1 TO ROLE TAPIR_DEV_Func_A;
GRANT USAGE ON WAREHOUSE TAPIR_DEV_wh1 TO ROLE TAPIR_DEV_Func_B;

-- revoke the temporary grant
USE ROLE TAPIR_prod_sysadmin;
REVOKE USAGE ON DATABASE TAPIR_prod FROM ROLE TAPIR_dev_sysadmin;
REVOKE USAGE ON ALL SCHEMAS IN DATABASE TAPIR_PROD FROM ROLE TAPIR_DEV_sysadmin;
REVOKE ALL PRIVILEGES ON ALL TABLES IN DATABASE TAPIR_PROD FROM ROLE TAPIR_DEV_sysadmin;

--         For the clone to succeed, temporary access to the PROD environment is
--         granted to the DEV sysadmin role. This is needed as initially the DEV
--         sysadmin does not have any access to the PROD environment.
--         Once the clone operation is performed, the temporary access must be
--         revoked.

-- 10.2.3  Grant the database access roles to the account level functional
--         roles.
--         This is to ensure our functional roles have the correct privileges.

-- Now grant the DEV database roles to the DEV functional roles
USE ROLE TAPIR_dev_sysadmin;
USE DATABASE TAPIR_DEV;
GRANT DATABASE ROLE TAPIR_DEV.db_sysadmin TO ROLE TAPIR_dev_sysadmin;
GRANT DATABASE ROLE TAPIR_DEV.SCH1_RW TO ROLE TAPIR_dev_func_a;
GRANT DATABASE ROLE TAPIR_DEV.SCH2_RW TO ROLE TAPIR_dev_func_a;
GRANT DATABASE ROLE TAPIR_DEV.SCH1_RO TO ROLE TAPIR_dev_func_B;
GRANT DATABASE ROLE TAPIR_DEV.SCH2_RO TO ROLE TAPIR_dev_func_B;

-- Remove the special grant
USE ROLE arch_role;
REVOKE MANAGE GRANTS ON ACCOUNT FROM ROLE TAPIR_DEV_sysadmin;


-- 10.2.4  Now, examine the grants applied to the database.

-- Show the database grants
SHOW GRANTS ON DATABASE TAPIR_DEV;

--         You should see showing similar to the following diagram
--         The key privilege is OWNERSHIP. We can see that the owning role of
--         the database is TAPIR_DEV_SYSADMIN, this is exactly what we want.

-- 10.2.5  Also, examine the ownership of the schemas, we can examine SCH1 as an
--         example.

SHOW GRANTS ON SCHEMA TAPIR_DEV.SCH1;

--         You should see something similar to the following diagram:
--         This time the owning role is the database role is DB_SYSADMIN. This
--         database role was granted to the account level DEV_sysadmin role,
--         again, this is what we need;

-- 10.2.6  Next, examine the grants at table level.

SHOW GRANTS ON TABLE TAPIR_DEV.SCH1.tab1;

--         Again, the OWNERSHIP privilege as DB_SYSADMIN.

-- 10.2.7  Now, to ensure all works as expected, we can use a functional role.
--         Here try to see if you can query and insert some data.

-- test access
USE ROLE TAPIR_dev_func_a;
USE WAREHOUSE TAPIR_DEV_wh1;
SELECT * FROM TAPIR_dev.sch1.tab1;
SELECT * FROM TAPIR_dev.sch2.tab1;

INSERT INTO TAPIR_dev.sch2.tab1 VALUES (10,'DDD');

USE ROLE TAPIR_dev_sysadmin;
CREATE TABLE TAPIR_dev.sch1.tab2 (id int, desc varchar(10));


-- 10.3.0  The Challenge
--         One particular grant was omitted from the above scripts.

-- 10.3.1  Try the following SQL and see if it works.

USE ROLE TAPIR_dev_func_a;
INSERT INTO TAPIR_dev.sch1.tab2 VALUES (100,'ZZZZ');

--         This fails with the following error
--         What is needed to correct this?

-- 10.3.2  Please produce the DDL to get this issue fixed for role
--         TAPIR_dev_func_a.
--         At this point, FUTURE grants have not been given to the database
--         roles. Also, please note that the second table that was added above
--         is not considered a future table as we have just created it, as such
--         we will need to re-grant the current grants to the database roles
--         also.
--         The following SQL will resolve the issue.

-- ANSWER AVAILABLE IN ANSWER KEY


-- 10.3.3  Now test using the following SQL.

USE ROLE TAPIR_dev_func_A;
INSERT INTO TAPIR_dev.sch1.tab2 VALUES (100,'ZZZZ');
SELECT * FROM TAPIR_dev.sch1.tab2;

USE ROLE TAPIR_dev_func_B;
INSERT INTO TAPIR_dev.sch1.tab2 VALUES (200,'YYYY');
SELECT * FROM TAPIR_dev.sch1.tab2;

--         The insert statement using role TAPIR_dev_func_B will fail because
--         that role only has select privileges, the remaining statements should
--         of been successful.

-- 10.4.0  Tidy Up
--         Let’s clean up the objects and roles we created. The following sql
--         will perform this task:

USE ROLE TAPIR_PROD_sysadmin;
DROP DATABASE TAPIR_PROD CASCADE;
USE ROLE TAPIR_DEV_sysadmin;
DROP DATABASE TAPIR_DEV CASCADE;

USE ROLE arch_role;
DROP ROLE IF EXISTS TAPIR_PROD_sysadmin;
DROP ROLE IF EXISTS TAPIR_PROD_secadmin;


-- 10.5.0  Key Takeaways
--         In this lab you have learned
--         The implications of permissions when cloning at DB, Schema, or table
--         level.
--         How to use database roles to simplify the cloning process ensuring
--         the desired permissions are maintained.
--         How to show what roles and permissions exist against given objects.
