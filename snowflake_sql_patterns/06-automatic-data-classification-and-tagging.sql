
-- 6.0.0   Automatic Data Classification and Tagging
--         By the end of this lab, you will be able to:
--         - Use the existing Snowflake automatic data classification out the
--         box and apply tags to attributes
--         - Explain how to customize the automatic data classification process
--         used above

-- 6.1.0   Environment Set Up
--         To get started, create an environment we will use for this lab.
--         The environment that will be configured in the following way:
--         Owning ROLE: arch_role
--         Database Name TAPIR_Classify_DB;
--         Schemas
--         EDW
--         CLASSIFICATIONS
--         Tables
--         The training role has also been granted the following privileges from
--         ACCOUNTADMIN Note: You do not need to execute the following SQL it is
--         shown here for clarity

-- USE ROLE accountadmin;
-- GRANT DATABASE ROLE SNOWFLAKE.CORE_VIEWER TO ROLE arch_role;
-- GRANT DATABASE ROLE SNOWFLAKE.GOVERNANCE_VIEWER TO ROLE arch_role;
-- GRANT DATABASE ROLE snowflake.classification_admin TO ROLE arch_role;
-- GRANT APPLY TAG ON account TO ROLE arch_role;


-- 6.1.1   Set context and create the environment.
--         The following SQL can be used to create the environment, you need to
--         execute the following step

USE ROLE arch_role;
CREATE DATABASE TAPIR_classify_db;
CREATE SCHEMA TAPIR_classify_db.classifications;
CREATE SCHEMA TAPIR_classify_db.edw;

USE SCHEMA TAPIR_classify_db.edw;

CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh WAREHOUSE_SIZE = XSMALL;

CREATE OR REPLACE TABLE uk_taxpayer (
    ni_number VARCHAR(9),
    filing_status VARCHAR(1),
    nbr_exemptions NUMBER(2,0),
    lastname VARCHAR(30),
    firstname VARCHAR(30),
    street VARCHAR(30),
    town VARCHAR(50),
    city VARCHAR(50),
    post_code Varchar(10),
    home_phone varchar(20),
    cell_phone varchar(20),
    email VARCHAR(40),
    birthdate DATE,
    taxpayer_type VARCHAR(30),
    corp_taxpayer_name VARCHAR(50),
    corp_taxpayer_effective_tax_rate_pct number(2,0) NULL
);

-- Create the file format

CREATE OR REPLACE FILE FORMAT ff_csv_tax
TYPE=csv
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = '';

--         This next section is used to load sample data into the UK_Taxpayer
--         table.

-- 6.1.2   Load the data into the table.
--         Using a CSV file located in a stage, load the file into the table.
--         The following SQL will perform that task

COPY INTO uk_taxpayer
FROM
(
    Select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
        ,$16::number(2,0)
    from @TRAINING_DB.TRAININGLAB.DATASETS_STAGE/arch/tax_data/uk_taxpayer.csv
)
FILE_FORMAT =  ff_csv_tax
ON_ERROR = 'CONTINUE'
FORCE = true;

--         This should load 2000 rows into the table.

-- 6.2.0   Standard Classification
--         Now that the table is populated, let’s run the standard
--         classification. For this, we can execute the function
--         SYSTEM$CLASSIFY.
--         The follow SQL can be used

-- Ensure the context
USE ROLE arch_role;
USE SCHEMA TAPIR_classify_db.edw;

-- Call the classify function
CALL SYSTEM$CLASSIFY('TAPIR_classify_db.edw.uk_taxpayer',null);

-- This statement can be used to retrieve the results at a later stage if required
SELECT SYSTEM$GET_CLASSIFICATION_RESULT('TAPIR_classify_db.edw.uk_taxpayer');


-- 6.2.1   Examine the Standard Classification results.
--         Let’s examine the results of the classification. In this exercise,
--         we’re going to focus on the column NI_NUMBER. The diagram below shows
--         a snippet of the CLASSIFY results:
--         The results show that the NI_Number has not been classified.
--         The next step is to create a custom rule so that we can classify the
--         data correctly.
--         This example, the intention is to classify NI_Number as a privacy
--         category SESNITIVE

-- 6.3.0   Build a Custom Rule
--         The first step here is to create a custom classifier. A customer
--         classifier is an object similar to that of an Object Oriented (OO)
--         object.
--         The similarities are that the customer classifier has methods.
--         !ADD_REGEX
--         !DELETE_CATEGORY
--         !LIST
--         Where a custom classifier differs to that of an OO object, is that it
--         is permanent and does not need to be instantiated.

-- 6.3.1   Create a custom classifier called Tax Classifier.

USE SCHEMA TAPIR_classify_db.classifications;

CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER Tax_Classifier();

--         The SHOW command can be used to display available classifiers.

-- 6.3.2   Execute the SHOW command to see available classifiers.

SHOW SNOWFLAKE.DATA_PRIVACY.CUSTOM_CLASSIFIER;

--         The results of the above command should look like

-- 6.3.3   Next step is to create a REG_EX method for identifying the NI_Number.

CALL Tax_Classifier!ADD_REGEX(
  'UK_TAX_IDENTIFIER',
  'Sensitive',
  '^([a-zA-Z]){2}( )?([0-9]){2}( )?([0-9]){2}( )?([0-9]){2}( )?([a-zA-Z]){1}?$',
  'NI.*',
  'RegEx for UK NI Numbers'
);

--         Parameters
--         UK_TAX_IDENTIFIER - This is the semantic category
--         Sensitive - This is the privacy category
--         ^([a-zA-Z]){2}( )?([0-9]){2}( )?([0-9]){2}( )?([0-9]){2}(
--         )?([a-zA-Z]){1}?$
--         This is the regular expression that reads
--         Begin with two letters
--         Followed by an optional space
--         Followed by 2 numbers
--         Followed by an optional space
--         Followed by 2 numbers
--         Followed by an optional space
--         Followed by 2 numbers
--         Ends with a letter
--         ’NI.*’ - This is an optional parameter. Its again a regular
--         expression and is used to identify column names.
--         RegEx for UK NI Numbers - This is an optional comment

-- 6.3.4   Display custom classifiers.
--         The !LIST method can be used to list the Reg-Ex methods
--         The following SQL can be used

SELECT Tax_Classifier!LIST();

--         The output of this SQL is in JSON form and will look like

-- 6.3.5   Execute custom classifiers - No Tags.
--         Execute the custom classifier. Here we will execute this the first
--         time so that we can review the results and then the second execution
--         will automatically associate the tags.
--         To execute the custom classifier, the following SQL can be executed.

CALL SYSTEM$CLASSIFY('TAPIR_classify_db.edw.uk_taxpayer',  {'custom_classifiers': ['Tax_Classifier']});

--         The results will show that the column NI_NUMBER has been classified
--         and should look like
--         We could customize rules if required. However, we will not do that as
--         part of this exercise.

-- 6.3.6   Execute custom classifiers - Create Tags.
--         To automatically create the tags, the following SQL can be used.

CALL SYSTEM$CLASSIFY('TAPIR_classify_db.edw.uk_taxpayer', {'custom_classifiers': ['Tax_Classifier'], 'auto_tag':true});


-- 6.3.7   Now lets examine the tags that have been created.

SELECT *
FROM TABLE(
  TAPIR_classify_db.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
    'TAPIR_classify_db.edw.uk_taxpayer',
    'table'
));

SELECT * FROM table(result_scan(last_query_id()))
WHERE "COLUMN_NAME" IN ('NI_NUMBER') ORDER BY COLUMN_NAME;

--         The result should look
--         Here we can see that two tags are created.
--         The PRIVACY_CATEGORY tag and the
--         SEMANTIC_CATEGORY tag.

-- 6.3.8   Tidy up the lab.

-- Drop the Objects
DROP DATABASE TAPIR_classify_db cascade;

-- Suspend and Resize your warehouse.
ALTER WAREHOUSE TAPIR_arch_wh SUSPEND;
ALTER WAREHOUSE TAPIR_arch_wh SET
  WAREHOUSE_SIZE = XSMALL;


-- 6.4.0   Key Takeaways
--         Standard classification may not classify the data correctly straight
--         out of the box. However, you can create a custom classifier, like we
--         did for the U.K. NI_NUMBER column.
--         You can execute the custom classifier first to review the results and
--         then execute setting Auto_tag:true to create the tags automatically.
