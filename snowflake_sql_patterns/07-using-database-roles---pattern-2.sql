
-- 7.0.0   Using Database Roles - Pattern 2
--         By the end of this lab you will be able to:
--         - Using the requirements, build an RBAC script based on database
--         roles - design pattern II.
--         - Create the database, schemas and virtual warehouses
--         - Define functional / user roles and ACCESS according to the
--         requirements
--         - Build the RBAC hierarchy between access roles, functional/user
--         roles, environment roles and the users
--         - Grant the functional/user to yourself for testing
--         - Understand the outcome of granting access from Access roles to User
--         roles
--         - Test access by switching roles, creating and querying tables
--         - Create a Table using one role which is instantly available for READ
--         access by another role
--         - Understand which SHOW commands can be used to quickly verify and
--         audit access to objects
--         What is Pattern 2? You saw a design called pattern 1 during the
--         lecture. We then refined that pattern by removing the database
--         functional roles; they don’t matter and called it pattern 2.
--         As such, the best practice role-based access control (RBAC) design is
--         Database Roles Pattern 2.

-- 7.1.0   Review the Requirements

-- 7.1.1   Review the following diagram to verify the access requirements needed
--         in this case.
--         The above requirements indicate:
--         Three Functional Roles ELT_SVC , ANALYST and DATA_SCI
--         ELT_SVC requires Read/Write access to all schemas plus USAGE access
--         to virtual warehouse ELT_WH
--         ANALYST requires read access to the schemas MART and EDW but no
--         access to RAW, plus USAGE access to virtual warehouse ANALYST_WH
--         DATA_SCI requires read/write access to the RAW schema and read only
--         against EDW and MART schema. USAGE privileges for virtual warehouse
--         DATA_SCI_WH which is of type snowpark optimized.
--         You have been tasked with creating an environment that matches the
--         supplied requirements. You are responsible for creating the
--         environments roles, database and schemas, you are also responsible
--         for setting up new users and creating the User/Functional roles and
--         required access to these.
--         Having set up access, you need to test each User role works as
--         expected and explore the Snowflake SHOW commands used to verify and
--         audit access.

-- 7.2.0   RBAC Design
--         This section will guide you through the design of the RBAC
--         implementation.

-- 7.2.1   Design of the Database and Schema Access.
--         The diagram below shows database, schemas and Access roles which will
--         be built during this lab.
--         At first this can seem a little overwhelming and complex but when
--         broken down into steps, it is rather straight forward. The Snowflake
--         SYSADMIN, SECURITYADMIN and the environment roles roles have been
--         purposely omitted from the diagram but will be included in the
--         scripting.
--         What the diagram is showing is.
--         Each schema (RAW, EDW, MART) has three access roles associated
--         DB Access Role for read only access (<Schema>_RO)
--         DB Access Role for read-write access (<Schema>_RW)
--         DB Access Role for full privileges (<Schema>_FULL)
--         The DB access roles are then granted relevant privileges. For example
--         the DB access role RAW_RO is granted select access to all tables and
--         views in the schema RAW.
--         Then, the access roles are granted to the account level functional
--         roles E.g. Role PROD_ANALYST is granted the following DB access roles
--         RAW_RO (Read Only access to the objects in the RAW schema)
--         EDW_RO (Read Only access to the objects in the EDW schema)
--         MART_RO (Read Only access to the objects in the MART schema)
--         These database roles will eventually be granted to the account level
--         functional roles, which in turn are then granted to the users.

-- 7.2.2   Design of the Virtual Warehouse Access.
--         The next diagram shows which User/Functional roles which will be
--         built for virtual data warehouses.
--         Again, this diagram can seem a little overwhelming, however it works
--         in a similar manner as the above, only instead of databases and
--         schemas, we are dealing with virtual warehouses.
--         The requirements shows three virtual warehouses, one for ELT work,
--         one for the data science work and the other for ANALYST work. Two
--         access roles are created for each virtual warehouse. One for usage
--         privileges and the other for full control privileges.
--         In this case only the usage roles are granted to the functional roles
--         and the full usage roles are granted to the environment SYSADMIN
--         role.
--         Please notice that for Virtual warehouses, we use account level
--         roles, this is because Virtual Warehouses are account level objects.
--         Note: Where the diagram references the environment (eg. PROD), we
--         will build these using your animal login name (eg. TIGER_PROD).

-- 7.2.3   You will be creating the deployment:
--         Notice this has four main sections:
--         Create the environment roles and DB creation.
--         Create the functional/user roles and all access roles
--         Create the role hierarchy
--         Create the warehouses, schemas and grant the object privileges to the
--         access roles
--         Create the tables
--         Be aware, step 3 (Access roles and grants) has been carefully crafted
--         to ensure it is repeatable. Additional grants for different object
--         types (in addition to tables and views) can be added or existing ones
--         removed, but be careful to test any changes.
--         This SQL code is not intended as a complete RBAC solution, but to
--         help provide a framework to deploy a scalable and simple
--         architecture.
--         Lets start building an environment to meet the specified
--         requirements.

-- 7.2.4   Part 1 - Environment Roles and Database Creation.
--         Execute the following SQL to create the environment roles:

   USE ROLE arch_role;

   CREATE OR REPLACE ROLE TAPIR_prod_secadmin;
   CREATE OR REPLACE ROLE TAPIR_prod_sysadmin;

   GRANT CREATE ROLE ON ACCOUNT TO TAPIR_prod_secadmin;
   GRANT ROLE TAPIR_prod_secadmin TO ROLE arch_role;
   GRANT ROLE TAPIR_prod_sysadmin TO ROLE arch_role;

   GRANT CREATE DATABASE ON ACCOUNT TO TAPIR_prod_sysadmin;
   GRANT CREATE WAREHOUSE ON ACCOUNT TO TAPIR_prod_sysadmin;

   -- Grant environment roles to your login

   GRANT ROLE TAPIR_prod_sysadmin TO USER TAPIR;
   GRANT ROLE TAPIR_prod_secadmin TO USER TAPIR;

   -- Create the database
   USE ROLE TAPIR_prod_sysadmin;
   CREATE OR REPLACE DATABASE TAPIR_prod DATA_RETENTION_TIME_IN_DAYS = 1;

   -- Secadmin Role requires the grant to create database roles and usage on the database
   GRANT CREATE DATABASE ROLE ON DATABASE TAPIR_PROD TO ROLE TAPIR_PROD_SECADMIN;
   GRANT USAGE ON DATABASE TAPIR_PROD TO ROLE TAPIR_PROD_SECADMIN;


-- 7.2.5   Part 2a - Create the account level functional roles.
--         Execute the following SQL to create the functional roles

   USE ROLE TAPIR_prod_secadmin;

   -- Functional Roles
   CREATE ROLE IF NOT EXISTS TAPIR_prod_elt_svc;
   CREATE ROLE IF NOT EXISTS TAPIR_prod_analyst;
   CREATE ROLE IF NOT EXISTS TAPIR_prod_data_sci;


-- 7.2.6   Part 2b - Create the database level access roles.

   -- Switch to environment secadmin role to create database roles
   USE ROLE TAPIR_prod_secadmin;
   USE DATABASE TAPIR_prod;

   -- Database Access Roles
   CREATE DATABASE ROLE IF NOT EXISTS _raw_ro;
   CREATE DATABASE ROLE IF NOT EXISTS _raw_rw;
   CREATE DATABASE ROLE IF NOT EXISTS _raw_sfull;
   CREATE DATABASE ROLE IF NOT EXISTS _edw_ro;
   CREATE DATABASE ROLE IF NOT EXISTS _edw_rw;
   CREATE DATABASE ROLE IF NOT EXISTS _edw_sfull;
   CREATE DATABASE ROLE IF NOT EXISTS _mart_ro;
   CREATE DATABASE ROLE IF NOT EXISTS _mart_rw;
   CREATE DATABASE ROLE IF NOT EXISTS _mart_sfull;


-- 7.2.7   Part 2c - Create the account level warehouse access roles.

   -- Warehouse Access Roles
   USE ROLE TAPIR_prod_secadmin;
   CREATE ROLE IF NOT EXISTS _TAPIR_PROD_ELT_WH_WU;
   CREATE ROLE IF NOT EXISTS _TAPIR_PROD_ELT_WH_WFULL;
   CREATE ROLE IF NOT EXISTS _TAPIR_PROD_ELT_WH_ALL;
   CREATE ROLE IF NOT EXISTS _TAPIR_PROD_ANALYST_WH_WU;
   CREATE ROLE IF NOT EXISTS _TAPIR_PROD_ANALYST_WH_WFULL;
   CREATE ROLE IF NOT EXISTS _TAPIR_PROD_ANALYST_WH_ALL;
   CREATE ROLE IF NOT EXISTS _TAPIR_PROD_DATA_SCI_WH_WU;
   CREATE ROLE IF NOT EXISTS _TAPIR_PROD_DATA_SCI_WH_WFULL;
   CREATE ROLE IF NOT EXISTS _TAPIR_PROD_DATA_SCI_WH_ALL;


-- 7.2.8   Part 3 - Create the role hierarchy.

-- Grant account level functional roles to TAPIR_prod_sysadmin
   USE ROLE TAPIR_prod_secadmin;
   GRANT ROLE TAPIR_prod_elt_svc TO ROLE TAPIR_prod_sysadmin;
   GRANT ROLE TAPIR_prod_analyst TO ROLE TAPIR_prod_sysadmin;
   GRANT ROLE TAPIR_prod_data_sci TO ROLE TAPIR_prod_sysadmin;

-- Grant all the database level access roles to TAPIR_prod_sysadmin
   GRANT DATABASE ROLE _raw_ro TO ROLE TAPIR_prod_sysadmin;
   GRANT DATABASE ROLE _raw_rw TO ROLE TAPIR_prod_sysadmin;
   GRANT DATABASE ROLE _raw_sfull TO ROLE TAPIR_prod_sysadmin;
   GRANT DATABASE ROLE _edw_ro TO ROLE TAPIR_prod_sysadmin;
   GRANT DATABASE ROLE _edw_rw TO ROLE TAPIR_prod_sysadmin;
   GRANT DATABASE ROLE _edw_sfull TO ROLE TAPIR_prod_sysadmin;
   GRANT DATABASE ROLE _mart_ro TO ROLE TAPIR_prod_sysadmin;
   GRANT DATABASE ROLE _mart_rw TO ROLE TAPIR_prod_sysadmin;
   GRANT DATABASE ROLE _mart_sfull TO ROLE TAPIR_prod_sysadmin;

-- Grant access roles to functional roles
   GRANT DATABASE ROLE _raw_rw to role TAPIR_prod_elt_svc;
   GRANT DATABASE ROLE _raw_rw to role TAPIR_prod_data_sci;
   GRANT DATABASE ROLE _edw_rw to role TAPIR_prod_elt_svc;
   GRANT DATABASE ROLE _edw_ro to role TAPIR_prod_analyst;
   GRANT DATABASE ROLE _edw_ro to role TAPIR_prod_data_sci;
   GRANT DATABASE ROLE _mart_rw to role TAPIR_prod_elt_svc;
   GRANT DATABASE ROLE _mart_ro to role TAPIR_prod_analyst;
   GRANT DATABASE ROLE _mart_ro to role TAPIR_prod_data_sci;

-- Grant warehouse access roles to TAPIR_prod_sysadmin
   USE ROLE TAPIR_prod_secadmin;
   GRANT ROLE _TAPIR_prod_elt_wh_wu TO ROLE TAPIR_prod_sysadmin;
   GRANT ROLE _TAPIR_prod_elt_wh_wfull TO ROLE TAPIR_prod_sysadmin;
   GRANT ROLE _TAPIR_prod_elt_wh_all TO ROLE TAPIR_prod_sysadmin;
   GRANT ROLE _TAPIR_prod_analyst_wh_wu TO ROLE TAPIR_prod_sysadmin;
   GRANT ROLE _TAPIR_prod_analyst_wh_wfull TO ROLE TAPIR_prod_sysadmin;
   GRANT ROLE _TAPIR_prod_analyst_wh_all TO ROLE TAPIR_prod_sysadmin;
   GRANT ROLE _TAPIR_prod_data_sci_wh_wu TO ROLE TAPIR_prod_sysadmin;
   GRANT ROLE _TAPIR_prod_data_sci_wh_wfull TO ROLE TAPIR_prod_sysadmin;
   GRANT ROLE _TAPIR_prod_data_sci_wh_all TO ROLE TAPIR_prod_sysadmin;

-- Grant virtual warehouse access roles to functional roles
   GRANT ROLE _TAPIR_prod_elt_wh_wu TO ROLE TAPIR_prod_elt_svc;
   GRANT ROLE _TAPIR_prod_analyst_wh_wu TO ROLE TAPIR_prod_analyst;
   GRANT ROLE _TAPIR_prod_data_sci_wh_wu TO ROLE TAPIR_prod_data_sci;


-- 7.2.9   Part 4 - Create the warehouses, schemas and grant the object
--         privileges to the access roles.
--         Execute the following SQL to create the virtual warehouses, schemas
--         and grant the object privileges. Please note that the object
--         privilege grants given in the SQL below are for tables and views
--         only. Additional grants will be required for other object types such
--         as Stages, File Formats etc.

-- Grant db usage to access roles
   USE ROLE TAPIR_PROD_SYSADMIN;
   GRANT USAGE ON DATABASE TAPIR_prod TO DATABASE ROLE _raw_ro;
   GRANT USAGE ON DATABASE TAPIR_prod TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON DATABASE TAPIR_prod TO DATABASE ROLE _raw_sfull;
   GRANT USAGE ON DATABASE TAPIR_prod TO DATABASE ROLE _edw_ro;
   GRANT USAGE ON DATABASE TAPIR_prod TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON DATABASE TAPIR_prod TO DATABASE ROLE _edw_sfull;
   GRANT USAGE ON DATABASE TAPIR_prod TO DATABASE ROLE _mart_ro;
   GRANT USAGE ON DATABASE TAPIR_prod TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON DATABASE TAPIR_prod TO DATABASE ROLE _mart_sfull;

--  Create the warehouses
   CREATE WAREHOUSE IF NOT EXISTS TAPIR_prod_elt_wh WITH
        WAREHOUSE_TYPE = 'STANDARD'
        warehouse_size='SMALL'
       ,scaling_policy='standard'
       ,min_cluster_count=1
       ,max_cluster_count=1
       ,auto_suspend=300
       ,auto_resume=true
       ,initially_suspended=true;

   CREATE WAREHOUSE IF NOT EXISTS TAPIR_prod_analyst_wh WITH WAREHOUSE_TYPE = 'STANDARD'
       warehouse_size='SMALL'
       ,scaling_policy='standard'
       ,min_cluster_count=1
       ,max_cluster_count=1
       ,auto_suspend=300
       ,auto_resume=true
       ,initially_suspended=true;

   CREATE WAREHOUSE IF NOT EXISTS TAPIR_prod_data_sci_wh WITH WAREHOUSE_TYPE='SNOWPARK-OPTIMIZED'
        warehouse_size='medium'
       ,scaling_policy='standard'
       ,min_cluster_count=1
       ,max_cluster_count=1
       ,auto_suspend=300
       ,auto_resume=true
       ,initially_suspended=true;

-- Grant privileges on warehouses to access roles
   GRANT USAGE ON WAREHOUSE TAPIR_prod_elt_wh TO ROLE _TAPIR_prod_elt_wh_wu;
   GRANT USAGE, OPERATE,MODIFY ON WAREHOUSE TAPIR_prod_elt_wh TO ROLE _TAPIR_prod_elt_wh_wfull;
   GRANT ALL ON WAREHOUSE TAPIR_prod_elt_wh TO ROLE _TAPIR_prod_elt_wh_all;
   GRANT USAGE ON WAREHOUSE TAPIR_prod_analyst_wh TO ROLE _TAPIR_prod_analyst_wh_wu;
   GRANT USAGE, OPERATE,MODIFY ON WAREHOUSE TAPIR_prod_analyst_wh TO ROLE _TAPIR_prod_analyst_wh_wfull;
   GRANT ALL ON WAREHOUSE TAPIR_prod_analyst_wh TO ROLE _TAPIR_prod_analyst_wh_all;
   GRANT USAGE ON WAREHOUSE TAPIR_prod_data_sci_wh TO ROLE _TAPIR_prod_data_sci_wh_wu;
   GRANT USAGE, OPERATE,MODIFY ON WAREHOUSE TAPIR_prod_data_sci_wh TO ROLE _TAPIR_prod_data_sci_wh_wfull;
   GRANT ALL ON WAREHOUSE TAPIR_prod_data_sci_wh TO ROLE _TAPIR_prod_data_sci_wh_all;

--  Create the schemas
   CREATE SCHEMA IF NOT EXISTS TAPIR_prod.raw WITH MANAGED ACCESS data_retention_time_in_days=1 comment = '';
   CREATE SCHEMA IF NOT EXISTS TAPIR_prod.edw WITH MANAGED ACCESS data_retention_time_in_days=1 comment = '';
   CREATE SCHEMA IF NOT EXISTS TAPIR_prod.mart WITH MANAGED ACCESS data_retention_time_in_days=1 comment = '';

--  Grant ownership on objects to access roles
   GRANT OWNERSHIP ON ALL TABLES IN SCHEMA raw TO DATABASE ROLE _raw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA raw TO DATABASE ROLE _raw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL STAGES IN SCHEMA raw TO DATABASE ROLE _raw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL FILE FORMATS IN SCHEMA raw TO DATABASE ROLE _raw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL STREAMS IN SCHEMA raw TO DATABASE ROLE _raw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL TASKS IN SCHEMA raw TO DATABASE ROLE _raw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA raw TO DATABASE ROLE _raw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA raw TO DATABASE ROLE _raw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA raw TO DATABASE ROLE _raw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL TABLES IN SCHEMA edw TO DATABASE ROLE _edw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA edw TO DATABASE ROLE _edw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL STAGES IN SCHEMA edw TO DATABASE ROLE _edw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL FILE FORMATS IN SCHEMA edw TO DATABASE ROLE _edw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL STREAMS IN SCHEMA edw TO DATABASE ROLE _edw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL TASKS IN SCHEMA edw TO DATABASE ROLE _edw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA edw TO DATABASE ROLE _edw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA edw TO DATABASE ROLE _edw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA edw TO DATABASE ROLE _edw_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL TABLES IN SCHEMA mart TO DATABASE ROLE _mart_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL VIEWS IN SCHEMA mart TO DATABASE ROLE _mart_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL STAGES IN SCHEMA mart TO DATABASE ROLE _mart_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL FILE FORMATS IN SCHEMA mart TO DATABASE ROLE _mart_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL STREAMS IN SCHEMA mart TO DATABASE ROLE _mart_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL TASKS IN SCHEMA mart TO DATABASE ROLE _mart_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL SEQUENCES IN SCHEMA mart TO DATABASE ROLE _mart_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL FUNCTIONS IN SCHEMA mart TO DATABASE ROLE _mart_sfull REVOKE CURRENT GRANTS;
   GRANT OWNERSHIP ON ALL PROCEDURES IN SCHEMA mart TO DATABASE ROLE _mart_sfull REVOKE CURRENT GRANTS;



-- Grant privileges on objects to access roles
   GRANT SELECT ON ALL TABLES IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT SELECT ON ALL VIEWS IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT USAGE,READ ON ALL STAGES IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT USAGE ON ALL FILE FORMATS IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT SELECT ON ALL STREAMS IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT USAGE ON ALL FUNCTIONS IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT USAGE ON ALL PROCEDURES IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT USAGE ON SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON ALL TABLES IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT SELECT ON ALL VIEWS IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE,READ,WRITE ON ALL STAGES IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON ALL FILE FORMATS IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT SELECT ON ALL STREAMS IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT MONITOR, OPERATE ON ALL TASKS IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON ALL SEQUENCES IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON ALL FUNCTIONS IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON ALL PROCEDURES IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT ALL ON ALL TABLES IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON ALL VIEWS IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON ALL STAGES IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON ALL FILE FORMATS IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON ALL STREAMS IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT MONITOR,OPERATE ON ALL TASKS IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON ALL SEQUENCES IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON ALL FUNCTIONS IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON ALL PROCEDURES IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT SELECT ON ALL TABLES IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT SELECT ON ALL VIEWS IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT USAGE,READ ON ALL STAGES IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT USAGE ON ALL FILE FORMATS IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT SELECT ON ALL STREAMS IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT USAGE ON ALL FUNCTIONS IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT USAGE ON ALL PROCEDURES IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT USAGE ON SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON ALL TABLES IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT SELECT ON ALL VIEWS IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE,READ,WRITE ON ALL STAGES IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON ALL FILE FORMATS IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT SELECT ON ALL STREAMS IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT MONITOR, OPERATE ON ALL TASKS IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON ALL SEQUENCES IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON ALL FUNCTIONS IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON ALL PROCEDURES IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT ALL ON ALL TABLES IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON ALL VIEWS IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON ALL STAGES IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON ALL FILE FORMATS IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON ALL STREAMS IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT MONITOR,OPERATE ON ALL TASKS IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON ALL SEQUENCES IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON ALL FUNCTIONS IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON ALL PROCEDURES IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT SELECT ON ALL TABLES IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT SELECT ON ALL VIEWS IN SCHEMA MART TO DATABASE ROLE _mart_ro;
   GRANT USAGE,READ ON ALL STAGES IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT USAGE ON ALL FILE FORMATS IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT SELECT ON ALL STREAMS IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT USAGE ON ALL FUNCTIONS IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT USAGE ON ALL PROCEDURES IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT USAGE ON SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON ALL TABLES IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT SELECT ON ALL VIEWS IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE,READ,WRITE ON ALL STAGES IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON ALL FILE FORMATS IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT SELECT ON ALL STREAMS IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT MONITOR, OPERATE ON ALL TASKS IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON ALL SEQUENCES IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON ALL FUNCTIONS IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON ALL PROCEDURES IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT ALL ON ALL TABLES IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON ALL VIEWS IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON ALL STAGES IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON ALL FILE FORMATS IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON ALL STREAMS IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT MONITOR,OPERATE ON ALL TASKS IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON ALL SEQUENCES IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON ALL FUNCTIONS IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON ALL PROCEDURES IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON SCHEMA mart TO DATABASE ROLE _mart_sfull;

--  Grant future privileges on objects to access roles
   GRANT SELECT ON FUTURE TABLES IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT SELECT ON FUTURE VIEWS IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT USAGE,READ ON FUTURE STAGES IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT SELECT ON FUTURE STREAMS IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA raw TO DATABASE ROLE _raw_ro;
   GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON FUTURE TABLES IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT SELECT ON FUTURE VIEWS IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE,READ,WRITE ON FUTURE STAGES IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT SELECT ON FUTURE STREAMS IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT MONITOR, OPERATE ON FUTURE TASKS IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA raw TO DATABASE ROLE _raw_rw;
   GRANT ALL ON FUTURE TABLES IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON FUTURE VIEWS IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON FUTURE STAGES IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON FUTURE FILE FORMATS IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON FUTURE STREAMS IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT MONITOR,OPERATE ON FUTURE TASKS IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON FUTURE SEQUENCES IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON FUTURE FUNCTIONS IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON FUTURE PROCEDURES IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT ALL ON FUTURE PROCEDURES IN SCHEMA raw TO DATABASE ROLE _raw_sfull;
   GRANT SELECT ON FUTURE TABLES IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT SELECT ON FUTURE VIEWS IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT USAGE,READ ON FUTURE STAGES IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT SELECT ON FUTURE STREAMS IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA edw TO DATABASE ROLE _Edw_ro;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA edw TO DATABASE ROLE _edw_ro;
   GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON FUTURE TABLES IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT SELECT ON FUTURE VIEWS IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE,READ,WRITE ON FUTURE STAGES IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT SELECT ON FUTURE STREAMS IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT MONITOR, OPERATE ON FUTURE TASKS IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA edw TO DATABASE ROLE _edw_rw;
   GRANT ALL ON FUTURE TABLES IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON FUTURE VIEWS IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON FUTURE STAGES IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON FUTURE FILE FORMATS IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON FUTURE STREAMS IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT MONITOR,OPERATE ON FUTURE TASKS IN SCHEMA edw TO DATABASE ROLE _Edw_sfull;
   GRANT ALL ON FUTURE SEQUENCES IN SCHEMA edw TO DATABASE ROLE _edw_sfulL;
   GRANT ALL ON FUTURE FUNCTIONS IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON FUTURE PROCEDURES IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT ALL ON FUTURE PROCEDURES IN SCHEMA edw TO DATABASE ROLE _edw_sfull;
   GRANT SELECT ON FUTURE TABLES IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT SELECT ON FUTURE VIEWS IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT USAGE,READ ON FUTURE STAGES IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT SELECT ON FUTURE STREAMS IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA mart TO DATABASE ROLE _mart_ro;
   GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON FUTURE TABLES IN SCHEMA mart TO DATABASE ROLE _mart_rW;
   GRANT SELECT ON FUTURE VIEWS IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE,READ,WRITE ON FUTURE STAGES IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT SELECT ON FUTURE STREAMS IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT MONITOR, OPERATE ON FUTURE TASKS IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA mart TO DATABASE ROLE _mart_rw;
   GRANT ALL ON FUTURE TABLES IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON FUTURE VIEWS IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON FUTURE STAGES IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON FUTURE FILE FORMATS IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON FUTURE STREAMS IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT MONITOR,OPERATE ON FUTURE TASKS IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON FUTURE SEQUENCES IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON FUTURE FUNCTIONS IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON FUTURE PROCEDURES IN SCHEMA mart TO DATABASE ROLE _mart_sfull;
   GRANT ALL ON FUTURE PROCEDURES IN SCHEMA mart TO DATABASE ROLE _mart_sfull;


-- 7.2.10  Part 5 - Create the tables.
--         You now can create the database objects such as tables. We will
--         attempt to use the role TAPIR_PROD_SYSADMIN The following SQL will
--         create some tables

USE ROLE TAPIR_prod_sysadmin;
USE SCHEMA TAPIR_prod.raw;
CREATE TABLE sales_inventory_raw (id number, stock number);

USE SCHEMA TAPIR_prod.edw;
CREATE TABLE sales_inventory (id number, stock number);

USE SCHEMA TAPIR_prod.mart;
CREATE VIEW sales_inv
AS SELECT * FROM TAPIR_prod.edw.sales_inventory;


-- 7.3.0   Grant Roles to UserId and View Roles

-- 7.3.1   Grant the functional/user roles to your userId.
--         You need to grant the functional/user roles to your userid. The
--         following SQL will perform that task.

USE ROLE TAPIR_prod_secadmin;
GRANT ROLE TAPIR_prod_elt_svc TO USER TAPIR;
GRANT ROLE TAPIR_prod_analyst TO USER TAPIR;
GRANT ROLE TAPIR_prod_data_sci TO USER TAPIR;


-- 7.3.2   Open Admin->Users/Roles in a new browser window.
--         To do this mouse-over the Admin Icon in left navigation, move down to
--         Users&Roles Right mouse click and select Open Link in New Window.

-- 7.3.3   Then click Roles at top next to Users.

-- 7.3.4   Next search for your TAPIR_prod_sysadmin role and click it.

-- 7.3.5   Now in the graph select the ellipse to the right of your
--         TAPIR_prod_sysadmin role,

-- 7.3.6   Then select Focus on role.
--         You should now see 12 granted roles in the graph.

-- 7.4.0   Perform some Testing
--         At this point the ELT role should be able the only role that can
--         write to tables in the EDW schema.
--         Let’s review by using the show command.

-- 7.4.1   The following SQL will show what grants have been given to the role
--         TAPIR_PROD_ETL_SVC;

USE ROLE TAPIR_prod_sysadmin;
SHOW GRANTS TO ROLE TAPIR_prod_elt_svc;

--         From the above image that the role TAPIR_PROD_ELT_SVC has been
--         granted the database roles
--         - TAPIR_PROD_EDW_RW
--         - TAPIR_PROD_MART_RW
--         - TAPIR_PROD_RAW_RW

-- 7.4.2   Now we need to examine the privileges of the database role
--         TAPIR_PROD_EDW_RW.
--         The following SQL can be used

SHOW GRANTS TO DATABASE ROLE TAPIR_prod._edw_rw;

--         The result should look like
--         What you will notice is this SHOW command displays the existing
--         grants against the existing tables.

-- 7.4.3   To determine the future grants at object type level use.

SHOW FUTURE GRANTS TO DATABASE ROLE TAPIR_prod._edw_rw;

--         This will show something like
--         Focusing on the table type (Column Grant_on), you can see the
--         privileges granted.

-- 7.4.4   Use the ELT Role to insert some data.
--         We can now use the ELT role to insert some data. Execute the
--         following SQL, 7 rows should be inserted

USE ROLE TAPIR_prod_elt_svc;
USE SCHEMA TAPIR_prod.raw;
USE WAREHOUSE TAPIR_prod_elt_wh;

INSERT INTO sales_inventory_raw (ID, Stock) Values
(1,100),
(2,200),
(3,300),
(4,400),
(5,500),
(6,600),
(7,700);

USE SCHEMA TAPIR_prod.edw;
INSERT INTO sales_inventory
SELECT * FROM raw.sales_inventory_raw;


-- 7.4.5   Use the Analyst Role to view the data in the MART Schema.
--         We can use the analyst role to view the data in the MART schema.
--         Execute the following SQL, you should see 7 rows.

USE ROLE TAPIR_prod_analyst;
USE SCHEMA TAPIR_prod.mart;
USE WAREHOUSE TAPIR_prod_analyst_wh;

SELECT * FROM sales_inv;


-- 7.4.6   Tidy up the lab.
--         Important, let’s clean up the objects and roles we created. The
--         following SQL will perform this task:

-- Drop the Objects
USE ROLE TAPIR_prod_sysadmin;
DROP DATABASE TAPIR_prod cascade;
DROP WAREHOUSE TAPIR_prod_analyst_wh;
DROP WAREHOUSE TAPIR_prod_elt_wh;
DROP WAREHOUSE TAPIR_prod_data_sci_wh;

-- Remove the Roles
USE ROLE TAPIR_prod_secadmin;
DROP ROLE _TAPIR_prod_analyst_wh_wfull;
DROP ROLE _TAPIR_prod_analyst_wh_wu;
DROP ROLE _TAPIR_prod_elt_wh_wfull;
DROP ROLE _TAPIR_prod_elt_wh_wu;

-- Drop the functional / user roles
DROP ROLE TAPIR_prod_analyst;
DROP ROLE TAPIR_prod_elt_svc;
DROP ROLE TAPIR_prod_data_sci;

-- Drop the environment roles
USE ROLE arch_role;
DROP ROLE TAPIR_prod_secadmin;
DROP ROLE TAPIR_prod_sysadmin;


-- 7.5.0   Key Takeaways
--         - Account level roles should include the environment name in them,
--         whereas database level roles do not.
--         - While the RBAC framework may seem a little complex at first glance,
--         in reality it simplifies the ongoing process of maintaining access
--         controls. In particular, it makes it easy to deploy or alter access
--         for User Roles
--         - Applying grants at the schema level using Access roles simplifies
--         the process of auditing access as by default users have Read,
--         Read/Write or Full access privileges.
--         - Finally, using a predefined method and naming standard enables
--         scripting of deployment and access control.
