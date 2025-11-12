
-- 1.0.0   Deployment Considerations
--         By the end of this lab, you will be able to:
--         - Understand the importance of naming conventions
--         - Understand the implications of different deployment options
--         discussed in the lesson
--         - Understand the implication of cloning against coding with fully
--         qualified object names inside code/views versus not fully qualified
--         object names
--         - Practice cloning an environment and see the effects it has against
--         environments
--         The purpose of this lab is to re-enforce what you learned about
--         deployment considerations.

-- 1.1.0   Create Worksheets from SQL files
--         You’ll create a worksheet from the SQL file provided with your course
--         materials.

-- 1.1.1   Make sure you are on the Home page and have Worksheets selected under
--         Projects.

-- 1.1.2   In the upper right corner, next to Search, click the ellipsis (three
--         dots).

-- 1.1.3   In the list that appears, select Create Worksheet from SQL File.

-- 1.1.4   Navigate to the location where you downloaded the lab files. Locate
--         file starting with the number of this lab and click Open.
--         This will open a new worksheet, named after the file you opened.
--         It’s easier to use the keyboard shortcut to run commands in the
--         worksheet, than it is to mouse back and forth between the SQL pane
--         and the Run button. To use the keyboard shortcut, place your cursor
--         on the line you want to run and click either CTRL+Return (for
--         Windows) or CMD+Return (for macOS).

-- 1.2.0   Part1: Multiple Databases
--         We first need to build out the DATABASES and SCHEMAS to represent a
--         single ENVIRONMENT.
--         - We first need to build TEST environment
--         - After we will clone this to build a DEV (Development) environment
--         The first configuration looks like this:
--         In summary:
--         - A single environment. TEST
--         - Two Layers (TRANSFORMATION and CONSUMPTION). Short code: TRAN and
--         CONS
--         - The TRANSFORMATION database is: TEST_TRAN with schema EDW
--         - The CONSUMPTION database is: TEST_CONS with schema SALES_ANALYSIS
--         - The TEST_TRAN.EDW has a table of sales: SALE_DATA
--         - The TEST_CONS.SALES_ANALYSIS has a view of the SALE_DATA - called
--         SALES
--         - This means users in TEST_CONS.SALES_ANALYSIS.sales will view the
--         corresponding data in the table TEST_TRAN.EDW.sale_data

-- 1.2.1   We will now build out the databases, schemas, tables, and views.
--         The following script can be used to build the environment.

USE ROLE arch_role;
CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh WITH
    WAREHOUSE_SIZE = 'XSMALL'
    INITIALLY_SUSPENDED = TRUE
    AUTO_SUSPEND = 60;

USE WAREHOUSE TAPIR_arch_wh;

CREATE OR REPLACE DATABASE TAPIR_test_tran;
CREATE OR REPLACE SCHEMA   TAPIR_test_tran.edw;

CREATE OR REPLACE DATABASE TAPIR_test_cons;
CREATE OR REPLACE SCHEMA   TAPIR_test_cons.sales_analysis;

-- Create the table
CREATE OR REPLACE TABLE TAPIR_test_tran.edw.sale_data
   (Sales_Date    Date
   ,Customer_Name Varchar(100)
   ,Customer_Email Varchar(100)
   ,Item_Sold      VARCHAR(100)
   ,Price          NUMBER(10,2)
   ,Tax            NUMBER(10,2)
   ,Total_Price    NUMBER(10,2)
   );

-- Insert some rows
INSERT INTO TAPIR_test_tran.EDW.sale_data VALUES
(to_date('2025-03-01','YYYY-MM-DD'), 'Bilbo Baggins', 'bilbo.baggins@bagend.com', 'Bread',2.00 , 0.40 , 2.40),
(to_date('2025-03-02','YYYY-MM-DD'), 'Samwise Gamgee', 'samwise.gamgee@bagend.com', 'Apples',4.00 , 0.80 , 4.80);


-- Create the view in the CONSUMPTION area over the EDW data
CREATE OR REPLACE VIEW TAPIR_test_cons.sales_analysis.sales
AS
SELECT Sales_Date
    ,Customer_Email
    ,Item_Sold
    ,Total_Price
FROM TAPIR_test_tran.edw.sale_data;

--         The environment to match the diagram has now been built.

-- 1.2.2   Let’s test the view we just built.
--         The following SQL can be used:

SELECT *
FROM TAPIR_test_cons.sales_analysis.sales;

--         The following result should be displayed:
--         To confirm:
--         - We are querying the data in the TEST environment view
--         TAPIR_test_cons.sales_analysis.sales
--         - IE. The TEST: Environment. CONSUMPTION: Layer. SALES_ANALYSIS:
--         schema. SALES: view
--         - We should see two rows in the table:
--         - Bilbo Baggins
--         - Samwise Gamgee

-- 1.2.3   We now need to clone the TEST databases to create a DEV (DEVELOPMENT)
--         environment.
--         The following SQL can be used

CREATE OR REPLACE DATABASE TAPIR_dev_tran CLONE TAPIR_test_tran;
CREATE OR REPLACE DATABASE TAPIR_dev_cons CLONE TAPIR_test_cons;

--         These two clone statements have now cloned both the TEST databases to
--         be used for DEV purposes.

-- 1.2.4   Let’s now switch over to the DEVELOPMENT environment.
--         Initially, we will move to the DEV_tran EDW schema.

USE SCHEMA TAPIR_dev_tran.edw;


-- 1.2.5   Let’s insert another row of data into the table Sale_Data in the DEV
--         environment.
--         Remember, this is in the EDW schema within the TAPIR_DEV_TRAN
--         database.

INSERT INTO TAPIR_dev_tran.edw.sale_data
VALUES
(to_date('2025-03-03','YYYY-MM-DD'), 'Gandalf The Grey', 'gandalf.thegrey@bagend.com', 'Fake Beard',5.00 , 1.00 , 6.00);

--         Let us confirm we now have the additional row in the table we just
--         inserted.

-- 1.2.6   Check the table, the results should look like (3 Rows).

SELECT *
FROM TAPIR_dev_tran.edw.sale_data;

--         To confirm:
--         - We are querying the data in the DEV environment table
--         TAPIR_DEV_tran.EDW.sale_data
--         - IE. The DEV: environment. TRANSFORMED: layer. EDW: schema.
--         SALE_DATA: table
--         - We should see emails in the table:
--         - Bilbo Baggins
--         - Samwise Gamgee
--         - Gandalf The Grey

-- 1.2.7   Let us again verify the data in the VIEW is also correct.

SELECT *
FROM TAPIR_dev_cons.sales_analysis.sales;

--         Note: Instead of THREE rows we only see TWO rows.
--         Can you think why this has happened? Give it some thought and make a
--         mental note before you continue.
--         Now, let us try to diagnose the problem.
--         The result is definitely showing 2 rows:
--         To confirm:
--         - We are querying the data in the DEV environment view
--         TAPIR_DEV_cons.SALES_ANALYSIS.sales
--         - IE. The DEV: environment. CONSUMPTION: layer. SALES_ANALYSIS:
--         schema. SALES: view
--         - We should see emails in the table:
--         - Bilbo Baggins
--         - Samwise Gamgee
--         This is because we have a view in the DEV environment which is
--         pointing to the TEST database.
--         - IE. The view: TAPIR_DEV_cons.SALES_ANALYSIS.sales (In the DEV
--         environment)
--         - Is pointing to the table: TAPIR_TEST_edw.sales_data (In the TEST
--         environment)

-- 1.2.8   We can confirm this by querying the view definition:

SELECT get_ddl('VIEW','TAPIR_dev_cons.sales_analysis.sales');

--         Can you see the query:
--         SELECT Sales_Date     ,Customer_Email     ,Item_Sold     ,Total_Price
--         FROM TAPIR_test_tran.edw.sale_data;
--         We are querying data in DEV from TAPIR_test_tran.edw.sale_data
--         We need to redeploy this view to fix the problem.
--         But the new view needs to be the TAPIR_dev_cons database pointing
--         to the TAPIR_dev_tran database

-- 1.2.9   To correct the issue, we must modify the view and re-deploy it.

CREATE OR REPLACE VIEW TAPIR_dev_cons.sales_analysis.sales
AS
SELECT Sales_Date
    ,Customer_Email
    ,Item_Sold
    ,Total_Price
FROM TAPIR_dev_tran.edw.sale_data;


-- 1.2.10  Now, test the view again.

SELECT *
FROM TAPIR_dev_cons.sales_analysis.sales;

--         The results should now be 3 rows, as shown below:
--         To confirm:
--         - We are querying the data in the DEV environment view
--         TAPIR_DEV_cons.SALES_ANALYSIS.sales
--         - IE. The DEV: environment. CONSUMPTION: layer. SALES_ANALYSIS:
--         schema. SALES: view
--         - We should see emails in the table:
--         - Bilbo Baggins
--         - Samwise Gamgee
--         - Gandalf The Grey
--         - All now correct!
--         Note: In this example we simply re-built the view with the new
--         definition pointing to the correct ENVIRONMENT. Be aware, if you
--         deploy using this multiple database deployment you need to replace
--         Every reference to the DATABASE in:
--         - Views
--         - Stored Procedures
--         - User Defined Functions
--         - Tasks
--         - Code written in Python, Java or other languages
--         - Configuration files used by ETL packages
--         - And any other location which includes a hard-coded reference to the
--         DATABASE name

-- 1.3.0   Part 2: Single Database Deployment
--         The previous section demonstrated one of the challenges faced when we
--         use multiple databases, with a different database for each LAYER in
--         the environment.
--         In this section we will show an alternative method whereby we deploy
--         a SINGLE DATABASE for each environment, but use a naming convention
--         and a set of SCHEMAS to represent the LAYERS within the environment.
--         We will configure an environment that looks like the diagram below.
--         This time the database name will not be qualified in the code and we
--         will create new clones.

-- 1.3.1   Use the following SQL to build this environment.

CREATE OR REPLACE DATABASE TAPIR_test;
CREATE OR REPLACE SCHEMA TAPIR_test.tran_edw;

CREATE OR REPLACE TABLE TAPIR_test.tran_edw.sale_data
   (Sales_Date     Date
   ,Customer_Name  Varchar(100)
   ,Customer_Email Varchar(100)
   ,Item_Sold      VARCHAR(100)
   ,Price          NUMBER(10,2)
   ,Tax            NUMBER(10,2)
   ,Total_Price    NUMBER(10,2)
   );

INSERT INTO TAPIR_test.tran_edw.sale_data
VALUES
(to_date('2025-03-01','YYYY-MM-DD'), 'Bilbo Baggins', 'bilbo.baggins@bagend.com', 'Bread',2.00 , 0.40 , 2.40),
(to_date('2025-03-02','YYYY-MM-DD'), 'Samwise Gamgee', 'samwise.gamgee@bagend.com', 'Apples',4.00 , 0.80 , 4.80);


CREATE OR REPLACE SCHEMA TAPIR_test.cons_sales_analysis;

--
-- Here we create the view in the TEST database CONS_SALES_ANALYSIS (Consumption) schema
-- But we only reference the SCHEMA name, not the DATABASE name.
--
CREATE OR REPLACE VIEW TAPIR_test.cons_sales_analysis.sales
AS
SELECT Sales_Date
    ,Customer_Email
    ,Item_Sold
    ,Total_Price
FROM tran_edw.sale_data;


-- 1.3.2   Let’s check the view SALES in TEST environment.

USE SCHEMA TAPIR_test.cons_sales_analysis;
SELECT *
FROM sales;

--         The result should be 2 rows, as follows:
--         To confirm:
--         - We are querying the data in the TEST environment view
--         TAPIR_TEST.CONS_SALES_ANALYSIS.sales
--         - IE. The TEST: environment. CONSUMPTION: layer. SALES_ANALYSIS:
--         schema. SALES: view
--         - NOTE: This time the database name is: **TAPIR_TEST** and the
--         schemas are prefixed with a code indicating the layer:
--         - IE. TRAN_EDW and CONS_SALES_ANALYSIS
--         - We should see two rows in the table:
--         - Bilbo Baggins
--         - Samwise Gamgee

-- 1.3.3   Now, let’s create a clone of TEST and call it DEV.

CREATE OR REPLACE DATABASE TAPIR_dev
   CLONE TAPIR_test;


-- 1.3.4   As before, let’s insert an extra row into the SALE_DATA Table - again
--         in DEV environment.

USE SCHEMA TAPIR_dev.tran_edw;
INSERT INTO sale_data
VALUES
(to_date('2025-03-03','YYYY-MM-DD'), 'Gandalf The Grey', 'gandalf.thegrey@bagend.com', 'Fake Beard',5.00 , 1.00 , 6.00);


-- 1.3.5   Check we have 3 rows in the PHYSICAL TABLE (again in DEV
--         environment).

USE SCHEMA TAPIR_dev.tran_edw;
SELECT *
FROM sale_data;

--         The result should now be 3 rows of data.
--         To confirm:
--         - We are querying the data in the DEV environment TABLE
--         TAPIR_dev.tran_edw.sale_data
--         - IE. The DEV: environment. TRANSFORMED: layer. EDW: schema.
--         SALES_DATA: table
--         - We should see emails in the table:
--         - Bilbo Baggins
--         - Samwise Gamgee
--         - Gandalf The Grey
--         - All correct!
--         So we can confirm there are three rows in the TABLE in the DEV
--         environment.

-- 1.3.6   Now let’s verify there are three rows in the VIEW.

USE SCHEMA TAPIR_dev.cons_sales_analysis;
SELECT *
FROM sales;

--         Result should be:
--         To confirm:
--         - We are querying the data in the DEV environment view
--         TAPIR_dev.cons_sales_analysis.sales
--         - IE. The DEV: environment. CONSUMPTION: layer. SALES_ANALYSIS:
--         schema. SALES: view
--         - We should see emails in the table:
--         - Bilbo Baggins
--         - Samwise Gamgee
--         - Gandalf The Grey
--         - AGAIN - all correct!
--         This demonstrates that when using a single database environment and
--         NOT fully qualifying the objects, the existing DB code does NOT
--         require re-deployment.

-- 1.3.7   Cleanup items used in this lab.

USE ROLE arch_role;
-- cascade is the default, included here for verbosity
DROP DATABASE TAPIR_test_tran cascade;
DROP DATABASE TAPIR_test_cons cascade;
DROP DATABASE TAPIR_dev_tran cascade;
DROP DATABASE TAPIR_dev_cons cascade;
DROP DATABASE TAPIR_test cascade;
DROP DATABASE TAPIR_dev cascade;


-- 1.4.0   Key Takeaways
--         - We must be careful how to deploy DATABASES and SCHEMAS
--         - We need to organize objects by ENVIRONMENT and also LAYER to help
--         organize database objects
--         - We have two primary options to achieve this:
--         - A) Deploy multiple DATABASES with each LAYER in a different
--         DATABASE
--         - B) Deploy a single DATABASE with each LAYER in a different SCHEMA
--         (prefixing schema names to group them by LAYER so multiple schemas in
--         the same layer are displayed together)
--         - Each of these options have advantages and drawbacks
--         - If we deploy MULTIPLE DATABASES we need to fully qualify the
--         DATABASE.SCHEMA.NAME and replace the DATABASE name when we switch
--         between ENVIRONMENTS
--         - If we deploy a SINGLE DATABASE we can execute the code across
--         environments without redeploying views or replacing database names.
--         But we must be careful to avoid references to the DATABASE NAME
--         except for an initial use database xxx command.
