
-- 8.0.0   Dynamic Data Masking
--         By the end of this lab you will be able to:
--         - Tag sensitive data and set tag values
--         - How to identify sensitive data that has been tagged
--         - Create secure data roles that are relevant to data classifications
--         - Create a masking policy based on roles only
--         - Create a tag based masking policy

-- 8.1.0   Environment Set up
--         Firstly, create an environment in which we can use data masking.
--         The environment that will be configured will look as shown in the
--         diagram below:
--         Database Goverance_DB is used to hold all the policies in the schema
--         Policy and tags will be stored in the Tags schema.
--         Database Tax_DB contains the data in which masking policies will be
--         applied.
--         Examples of a role only based policy will be implemented, this will
--         later be replaced with a tag based policy.
--         The functions used in the Lab are
--         - current_available_roles() and
--         - SYSTEM$Get_Tag_On_Current_Column()
--         These functions are used in conjunction with secure data roles. Role
--         inheritance does not play a part in this Lab but other functions such
--         as is_role_in_session could be used if role inheritance (or secondary
--         roles) were to be included.

-- 8.1.1   To build this environment, use the following SQL.

USE ROLE arch_role;

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS TAPIR_arch_wh;
-- Goverance Database
CREATE OR REPLACE DATABASE TAPIR_Goverance_db;
-- Schema for policies
CREATE OR REPLACE  SCHEMA TAPIR_Goverance_db.Policies;
-- Schemaa for Tags
CREATE OR REPLACE  SCHEMA TAPIR_Goverance_db.Tags;

-- Database main data
CREATE OR REPLACE DATABASE TAPIR_tax_db;
-- Schema for tax tables
CREATE OR REPLACE SCHEMA TAPIR_tax_db.taxation;

USE SCHEMA TAPIR_tax_db.taxation;
CREATE OR REPLACE TABLE taxpayer as select * from training_tax_db.taxschema.taxpayer;
CREATE OR REPLACE TABLE taxpayer_wages as select * from training_tax_db.taxschema.taxpayer_wages;


-- 8.2.0   Classify the data using tags.
--         We can classify the columns in the taxpayer table as follows.

-- 8.2.1   Create the secure data roles (SDR’s).
--         Secure Data Roles will be used only for data governance purposes.
--         These roles do not get granted any privileges to database objects
--         such as tables.
--         The SDR’s can be created with the following SQL:

CREATE ROLE _sdr_TAPIR_highly_confidential;
CREATE ROLE _sdr_TAPIR_confidential;


-- 8.3.0   Create the tags for classification
--         Here we will create some tags that can be used to identify sensitive
--         data When creating tags, keep in mind that you will probably want to
--         use them elsewhere, E.g. an Email tag could be used to tag any tables
--         with Email columns.
--         Also this step would typically be performed by the Governance team,
--         not necessarily the Snowflake system admin

-- 8.3.1   The following SQL can be used to implement masking on an email
--         address.

USE ROLE arch_role;
USE SCHEMA TAPIR_Goverance_db.Tags;
CREATE OR REPLACE TAG ID_Tag
   ALLOWED_VALUES 'highly_confidential','confidential','public';
CREATE OR REPLACE TAG Name_Tag
   ALLOWED_VALUES 'highly_confidential','confidential','public';
CREATE OR REPLACE TAG Street_Tag
   ALLOWED_VALUES 'highly_confidential','confidential','public';
CREATE OR REPLACE TAG Phone_Tag  
   ALLOWED_VALUES 'highly_confidential','confidential','public';
CREATE OR REPLACE TAG Email_Tag
   ALLOWED_VALUES 'highly_confidential','confidential','public';
CREATE OR REPLACE TAG DOB_Tag
   ALLOWED_VALUES 'highly_confidential','confidential','public';

--         At this point all we have done is create tags, however this is enough
--         information for the policy administrator to understand what policies
--         need to be created. If the policy administrator performed the
--         following SQL

USE ROLE arch_role;
USE SCHEMA TAPIR_Goverance_db.Tags;
SHOW TAGS;

--         From this output, it can be seen that policies are required for
--         Date Of Birth
--         First and last names
--         Emails
--         Tax_ID
--         Phone numbers
--         Street names
--         Later in this lab we will create policies for Emails

-- 8.3.2   Associate the tags to specific columns and assign a value for the
--         tag.
--         Here we will set tags against the columns and assign a value to the
--         tag for the given column. Again, this step would typically be
--         performed by the Governance team, not necessarily the Snowflake
--         system admin

USE ROLE arch_role;
USE SCHEMA TAPIR_Goverance_db.Tags;
ALTER TABLE TAPIR_tax_db.taxation.taxpayer Modify Column Taxpayer_ID
   SET TAG ID_Tag     = 'highly_confidential';
ALTER TABLE TAPIR_tax_db.taxation.taxpayer Modify Column LastName
   SET TAG Name_Tag   = 'confidential';
ALTER TABLE TAPIR_tax_db.taxation.taxpayer Modify Column FirstName
   SET TAG Name_Tag   = 'confidential';
ALTER TABLE TAPIR_tax_db.taxation.taxpayer Modify Column Street
   SET TAG Street_Tag = 'confidential';
ALTER TABLE TAPIR_tax_db.taxation.taxpayer Modify Column EMail
   SET TAG EMail_Tag  = 'highly_confidential';
ALTER TABLE TAPIR_tax_db.taxation.taxpayer Modify Column Home_Phone
   SET TAG Phone_Tag  = 'highly_confidential';
ALTER TABLE TAPIR_tax_db.taxation.taxpayer Modify Column Cell_Phone
   SET TAG Phone_Tag  = 'highly_confidential';
ALTER TABLE TAPIR_tax_db.taxation.taxpayer Modify Column Birthdate
   SET TAG DOB_Tag    = 'highly_confidential';


-- 8.3.3   Review the tagged data.
--         Suppose the governance team has performed all the tagging, this step
--         shows how to determine what has been tagged.
--         Here we will all tags set against the taxpayer table. The following
--         SQL can be used:

SELECT *
FROM TABLE(TAPIR_tax_db.information_schema.TAG_REFERENCES_ALL_COLUMNS
  ('TAPIR_tax_db.taxation.taxpayer','table'));

--         The following diagram shows what the output should look like, only
--         your TAPIR name will be shown instead of INSTRUCTOR1
--         Here we can see the specific values applied to the tags for the given
--         columns Suppose there are many tables in this schema, we don’t
--         particularly want to issue this command against every table, so its
--         good practice to perhaps put a tag on the table itself to indicate
--         there are columns of a sensitive nature.
--         Now that we have identified columns that have a sensitive nature,
--         then some masking policies can be created and applied to the columns.
--         It is generally the policy administrator who will create the policies

-- 8.4.0   Creating Policies
--         In this section we will create two types of policy.
--         Role Only masking policy
--         Tag based masking policy

-- 8.4.1   Create Role Only Policies.
--         Here we will create the masking policy for Email.
--         For this example assume the following requirements
--         If the user has been granted the highly_confidential SDR, then they
--         can see data that was tagged as highly confidential
--         If the user has been granted confidential, they will see a partial
--         email address
--         If the user has neither SDR, then the string ’*** Masked ***’ will be
--         shown
--         The following SQL can be used:

USE ROLE arch_role;
USE SCHEMA TAPIR_Goverance_db.Policies;

-- Role based policy
CREATE or REPLACE MASKING POLICY email_mask AS
(val string) returns string ->  
  CASE
    WHEN contains(current_available_roles(), '_SDR_TAPIR_HIGHLY_CONFIDENTIAL') THEN val
    WHEN contains(current_available_roles(), '_SDR_TAPIR_CONFIDENTIAL') THEN regexp_replace(val,'.+\@','*****@')
    ELSE '*** MASKED ***'
   END;


-- 8.4.2   Now we apply the policy to the Email column in the table.
--         The following SQL will perform that task.

ALTER TABLE TAPIR_tax_db.taxation.taxpayer MODIFY COLUMN email SET MASKING POLICY TAPIR_Goverance_db.Policies.email_mask;


-- 8.4.3   Lets test this.
--         Currently your user has not been granted any SDR roles, as such if we
--         select against the taxpayer table, the Email column should be masked.
--         Run the following SQL:

SELECT firstname, lastname, email  FROM TAPIR_tax_db.taxation.taxpayer;

--         The following output should be displayed:
--         Now lets grant your user the confidential SDR and re-run the query:

GRANT ROLE _sdr_TAPIR_confidential TO USER TAPIR;
SELECT firstname, lastname, email  FROM TAPIR_tax_db.taxation.taxpayer;

--         The following result should be displayed:
--         Now, lets test the highly_confidential SDR role and re-run the query
--         The following SQL can be used:

REVOKE ROLE _sdr_TAPIR_confidential FROM USER TAPIR;
GRANT ROLE _sdr_TAPIR_highly_confidential TO USER TAPIR;
SELECT firstname, lastname, email  FROM TAPIR_tax_db.taxation.taxpayer;

--         This time we see the full value of the email address, as shown below:
--         This is a fairly simple mechanism to implement, however, suppose the
--         EMail column was to be re-classified as confidential, then the logic
--         inside the policy would need to change to reflect that.
--         To ensure additional flexibility around re-classifying data, we can
--         use tag based policies instead. The next section will shown how this
--         is done

-- 8.4.4   Tag Based Policies.
--         Tag based policies offer a more flexible way of data governance and
--         re-classification of data. Before we start with tag based policies,
--         let’s drop the role based policy. The following SQL can be used:

USE ROLE arch_role;
ALTER TABLE TAPIR_tax_db.taxation.taxpayer MODIFY COLUMN email UNSET MASKING POLICY;
DROP MASKING POLICY TAPIR_Goverance_db.Policies.email_mask;

--         With tag based policies, we apply the policy to the tag and the tag
--         is applied to a given column in a table. Firstly lets create the tag
--         based masking policy. In this case the requirements are:
--         If the tag value is highy_confidential and the user has the sdr role
--         highly_confidential, the value can be seen
--         If the tag value is highy_confidential and the user has the sdr role
--         confidential then show a only the domain part of the email address
--         If the tag value is confidential and the user has the sdr role
--         highly_confidential or sdr role confidential, the value can be seen
--         None of the above, show ’*** Masked ***’
--         The following SQL shows how to create a tag based policy.

USE ROLE arch_role;
USE SCHEMA TAPIR_goverance_db.policies;
--ALter MAsking Policy email_mask set body ->
CREATE or REPLACE masking policy email_mask AS
 (val string) returns string ->  
  CASE
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAPIR_Goverance_db.Tags.Email_Tag') = 'highly_confidential' And
         contains(current_available_roles(), '_SDR_TAPIR_HIGHLY_CONFIDENTIAL') THEN val
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAPIR_Goverance_db.Tags.Email_Tag') = 'highly_confidential' And
         contains(current_available_roles(), '_SDR_TAPIR_CONFIDENTIAL') THEN regexp_replace(val,'.+\@','*****@')
    WHEN SYSTEM$GET_TAG_ON_CURRENT_COLUMN('TAPIR_Goverance_db.Tags.Email_Tag') = 'confidential' And  
         contains(current_available_roles(), '_SDR_TAPIR_CONFIDENTIAL') THEN val
    ELSE '*** MASKED ***'
   END;

--         The policy is now created, now to apply the policy to the tag. The
--         following SQL will perform this task. Please note the role being used
--         must have the privilege APPLY MASKING POLICY ON ACCCOUNT, in our case
--         arch_role has this

ALTER TAG TAPIR_goverance_db.tags.Email_Tag SET MASKING POLICY TAPIR_goverance_db.policies.email_mask;

--         Lets now test this, firstly lets ensure that your user does not have
--         any sdr roles. The following sql will perform that task

 REVOKE ROLE _sdr_TAPIR_highly_confidential FROM USER TAPIR;
 REVOKE ROLE _sdr_TAPIR_confidential FROM USER TAPIR;

--         Lets test with no sdr role, the email should be masked out Run the
--         following SQL

SELECT firstname, lastname, email  FROM TAPIR_tax_db.taxation.taxpayer;

--         The following output should be displayed:
--         Now lets grant your user the confidential SDR and re-run the query

GRANT ROLE _sdr_TAPIR_confidential TO USER TAPIR;
SELECT firstname, lastname, email  FROM TAPIR_tax_db.taxation.taxpayer;

--         The following result should be displayed:
--         Now, lets test the highly_confidential SDR role and re-run the query
--         The following SQL can be used:

REVOKE ROLE _sdr_TAPIR_confidential FROM USER TAPIR;
GRANT ROLE _sdr_TAPIR_highly_confidential TO USER TAPIR;
SELECT firstname, lastname, email  FROM TAPIR_tax_db.taxation.taxpayer;

--         This time we see the full value of the email address, as shown below

-- 8.4.5   Cleanup items used in this lab.

USE ROLE arch_role;
ALTER TAG TAPIR_goverance_db.tags.Email_Tag unset masking policy TAPIR_goverance_db.policies.email_mask;
DROP ROLE _sdr_TAPIR_highly_confidential;
DROP ROLE _sdr_TAPIR_confidential;
DROP DATABASE TAPIR_Goverance_db cascade;
DROP DATABASE TAPIR_tax_db cascade;


-- 8.5.0   Key Takeaways
--         - Before even considering what data policy masks are required, the
--         data must first be classified.
--         - A common mistake is to unset the policy from the column and run
--         create or replace, re-deploy the policy, and re-associate the policy
--         to the column once again. DO NOT DO THAT! Once the policy is
--         disassociated, no security for that column exists (other than RBAC)
--         This is dangerous. ALWAYS use ALTER POLICY instead.
