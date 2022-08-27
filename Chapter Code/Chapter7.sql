// Snowflake Definitive Guide 1st Edition by Joyce Kay Avila - August 2022
// ISBN-10 : 1098103823
// ISBN-13 : 978-1098103828
// Contact the author: https://www.linkedin.com/in/joycekayavila/
// Chapter 7: Implementing Data Governance, Account Security, and Data Protection and Recovery


// Page 228 - Prep Work
// Create new worksheet: Chapter7 Security, Protection, and Data Recovery
// Context setting - make sure role is set to SYSADMIN and COMPUTE_WH is the virtual warehouse

// Page 229 - create a new user
// Make sure to replace YOUREMAIL@EMAIL.COM with your own personal email address
USE ROLE USERADMIN;
CREATE OR REPLACE USER ADAM
PASSWORD = '123'
LOGIN_NAME=ADAM
DISPLAY_NAME=ADAM
EMAIL= ' YOUREMAIL@EMAIL.COM '
MUST_CHANGE_PASSWORD=TRUE;

//Page 229 - Create Users to be used in later examples
USE ROLE USERADMIN;
CREATE OR REPLACE ROLE HR_ROLE;
CREATE OR REPLACE ROLE AREA1_ROLE;
CREATE OR REPLACE ROLE AREA2_ROLE;

// Page 229 - Create some objects to be used in later examples
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
CREATE OR REPLACE DATABASE DEMO7_DB;
CREATE OR REPLACE SCHEMA TAG_LIBRARY;
CREATE OR REPLACE SCHEMA HRDATA;
CREATE OR REPLACE SCHEMA CH7DATA;
CREATE OR REPLACE TABLE DEMO7_DB.CH7DATA.RATINGS
(EMP_ID integer, RATING integer, DEPT_ID varchar, AREA integer);

// Page 229 -- Insert some arbitrary data
INSERT INTO DEMO7_DB.CH7DATA.RATINGS VALUES
(1, 77, '100', 1),
(2, 80, '100', 1),
(3, 72, '101', 1),
(4, 94, '200', 2),
(5, 88, '300', 3),
(6, 91, '400', 3);

// Page 230 - Grant some permissions to roles
USE ROLE SECURITYADMIN;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE HR_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AREA1_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE AREA2_ROLE;

// Page 230 -- Grant roles usage on the objects
GRANT USAGE ON DATABASE DEMO7_DB TO ROLE HR_ROLE;
GRANT USAGE ON DATABASE DEMO7_DB TO ROLE AREA1_ROLE;
GRANT USAGE ON DATABASE DEMO7_DB TO ROLE AREA2_ROLE;
GRANT USAGE ON SCHEMA DEMO7_DB.CH7DATA TO ROLE HR_ROLE;
GRANT USAGE ON SCHEMA DEMO7_DB.HRDATA TO ROLE HR_ROLE;
GRANT USAGE ON SCHEMA DEMO7_DB.CH7DATA TO ROLE AREA1_ROLE;
GRANT USAGE ON SCHEMA DEMO7_DB.CH7DATA TO ROLE AREA2_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DEMO7_DB.CH7DATA TO ROLE HR_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DEMO7_DB.CH7DATA TO ROLE AREA1_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA DEMO7_DB.CH7DATA TO ROLE AREA2_ROLE;

// Page 230 - Assign three roles to the new user and assign two custom roles back to SYSADMIN
GRANT ROLE HR_ROLE TO USER ADAM;
GRANT ROLE AREA1_ROLE TO USER ADAM;
GRANT ROLE AREA2_ROLE TO USER ADAM;
GRANT ROLE AREA1_ROLE TO ROLE SYSADMIN;
GRANT ROLE AREA2_ROLE TO ROLE SYSADMIN;

// Page 230 - Grant future priviliges to the HR Role
USE ROLE ACCOUNTADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DEMO7_DB.HRDATA TO ROLE HR_ROLE;
GRANT INSERT ON FUTURE TABLES IN SCHEMA DEMO7_DB.HRDATA TO ROLE HR_ROLE;
USE ROLE SYSADMIN;

// Page 236 - Number of queries each user has run
USE ROLE ACCOUNTADMIN;
SELECT USER_NAME, COUNT(*) USES FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
GROUP BY USER_NAME ORDER BY USES DESC;

// Page 236 - Most frequently used tables
SELECT OBJ.VALUE:objectName::STRING TABLENAME,
COUNT(*) USES FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
TABLE(FLATTEN(BASE_OBJECTS_ACCESSED)) OBJ GROUP BY TABLENAME ORDER BY USES DESC;

// Page 236 - What tables are being accessed, by whom, and how frequently
SELECT OBJ.VALUE:objectName::string TABLENAME, USER_NAME,
COUNT(*) USES FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
TABLE(FLATTEN(BASE_OBJECTS_ACCESSED)) OBJ GROUP BY 1, 2 ORDER BY USES DESC;

// Page 239 - Set the retention time for a database to 90 days
USE ROLE SYSADMIN;
ALTER DATABASE DEMO7_DB SET DATA_RETENTION_TIME_IN_DAYS = 90;

// Page 240 - Confirm that a table in the database has 90-day retention time
USE ROLE SYSADMIN;
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, RETENTION_TIME
FROM DEMO7_DB.INFORMATION_SCHEMA.TABLES;

// Page 240 - Take a look at the Ratings table as it exists before making change
SELECT * FROM DEMO7_DB.CH7DATA.RATINGS;

// Page 240 - Change all areas to 4 even though intent was just to change one value
USE ROLE SYSADMIN;
UPDATE DEMO7_DB.CH7DATA.RATINGS AREA SET AREA=4;

// Page 240 - Take a look at the Ratings table after the change was made
SELECT * FROM DEMO7_DB.CH7DATA.RATINGS;

// Page 241 - In order to use Time Travel to revert the table back to prior values, need to find out how far to travel back
// Try 5 minutes first but may need to adjust to perhaps 2 or 3 minutes
SELECT * FROM DEMO7_DB.CH7DATA.RATINGS at (offset => -60*5);

// Page 241 - Use time travel to revert the table back to prior values before change was made
// If changed value from 5 minutes to a different value in previous statement, use that new value
CREATE OR REPLACE TABLE DEMO7_DB.CH7DATA.RATINGS
AS SELECT * FROM DEMO7_DB.CH7DATA.RATINGS at (offset => -60*5);

// Page 241 - Take a look at the Ratings table now
SELECT * FROM DEMO7_DB.CH7DATA.RATINGS;

// Page 241 - Drop a table
DROP TABLE DEMO7_DB.CH7DATA.RATINGS;

// Page 241 - Take a look at the Ratings table now
// You'll see that the table no longer exists
SELECT * FROM DEMO7_DB.CH7DATA.RATINGS;

// Page 241 - Undrop the table
UNDROP TABLE DEMO7_DB.CH7DATA.RATINGS;

// Page 241 - Take a look at the Ratings table now
// You'll see that the table does now exist
SELECT * FROM DEMO7_DB.CH7DATA.RATINGS;

// Page 243 - How much storage is attributable to each section
USE ROLE ACCOUNTADMIN;
SELECT TABLE_NAME,ACTIVE_BYTES,TIME_TRAVEL_BYTES,FAILSAFE_BYTES
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

// Page 246 - Create a tag library, taxonomy of tags
USE ROLE SYSADMIN;
CREATE OR REPLACE SCHEMA DEMO7_DB.TAG_LIBRARY;

// Page 246 - Create tag level Classification
CREATE OR REPLACE TAG Classification;
ALTER TAG Classification set comment =
 "Tag Tables or Views with one of the following classification values:
 'Confidential', 'Restricted', 'Internal', 'Public'";
 
// Page 246 - Create tag level PII
CREATE OR REPLACE TAG PII;
ALTER TAG PII set comment = "Tag Tables or Views with PII with one or more
 of the following values: 'Phone', 'Email', 'Address'";
 
// Page 246 - Create tag level Sensitive PII 
CREATE OR REPLACE TAG SENSITIVE_PII;
ALTER TAG SENSITIVE_PII set comment = "Tag Tables or Views with Sensitive PII
 with one or more of the following values: 'SSN', 'DL', 'Passport',
'Financial', 'Medical'";

// Page 247 - Create an Employees table and insert some values into it
USE ROLE SYSADMIN;
CREATE OR REPLACE TABLE DEMO7_DB.HRDATA.EMPLOYEES (emp_id integer,
 fname varchar(50), lname varchar(50), ssn varchar(50), email varchar(50),
 dept_id integer, dept varchar(50));
INSERT INTO DEMO7_DB.HRDATA.EMPLOYEES (emp_id, fname, lname, ssn, email,
 dept_id, dept) VALUES
 (0, 'First', 'Last', '000-00-0000', 'email@email.com', '100', 'IT');
 

// Page 247 - Assign a tag at the table level
ALTER TABLE DEMO7_DB.HRDATA.EMPLOYEES
 set tag DEMO7_DB.TAG_LIBRARY.Classification="Confidential";
 
// Page 248 - Set tags at the column level 
ALTER TABLE DEMO7_DB.HRDATA.EMPLOYEES MODIFY EMAIL
 set tag DEMO7_DB.TAG_LIBRARY.PII = "Email";
ALTER TABLE DEMO7_DB.HRDATA.EMPLOYEES MODIFY SSN
 SET TAG DEMO7_DB.TAG_LIBRARY.SENSITIVE_PII = "SSN";
 
 // Page 248 - Use the SHOW ocmmand to see the details of the tags
 SHOW TAGS;
 
// Page 247 - See if a particular tag is associated wtih a specific table
SELECT SYSTEM$GET_TAG('Classification', 'DEMO7_DB.HRDATA.EMPLOYEES', 'table');

// Page 248 - See if a particular tag is associated with a specific column
SELECT SYSTEM$GET_TAG('SENSITIVE_PII', 'DEMO7_DB.HRDATA.EMPLOYEES.SSN',
 'column');

// Page 248 - See if a particular tag is associated with a specific column
SELECT SYSTEM$GET_TAG('SENSITIVE_PII', 'DEMO7_DB.HRDATA.EMPLOYEES.EMAIL',
 'column');
 
 
// Page 248 Audit all columns with a sensitive tag without a masking policy
// Note that Latency may be up to 2 hours for tag_references so you may need to revisit this statement later
USE ROLE ACCOUNTADMIN;
WITH column_with_tag
AS (SELECT object_name table_name,
 column_name column_name,
 object_database db_name,
 object_schema schema_name
 FROM snowflake.account_usage.tag_references
 WHERE tag_schema='TAG_LIBRARY'
 AND (tag_name='SENSITIVE_PII' OR tag_name = 'PII')
 AND column_name is not null),
 column_with_policy
AS (SELECT ref_entity_name table_name,
 ref_column_name column_name,
 ref_database_name db_name,
 ref_schema_name schema_name
 FROM snowflake.account_usage.policy_references
 WHERE policy_kind='MASKING POLICY')
 SELECT *
 FROM column_with_tag
 MINUS
 SELECT *
 FROM column_with_policy;
 
// Page 251 - Create a masking policy 
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE masking policy DEMO7_DB.HRDATA.emailmask
 AS (val string) returns string ->
CASE WHEN current_role() in ('HR_ROLE') THEN val
 ELSE '**MASKED**' END;
ALTER TABLE DEMO7_DB.HRDATA.EMPLOYEES modify column EMAIL
 set masking policy DEMO7_DB.HRDATA.emailmask; 


// Page 251 - Create another masking policy  
CREATE OR REPLACE masking policy DEMO7_DB.HRDATA.SSNmask
 AS (val string) returns string ->
    CASE WHEN current_role() in ('HR_ROLE') THEN val
    ELSE '**MASKED**' END;

ALTER TABLE DEMO7_DB.HRDATA.EMPLOYEES modify column SSN
 set masking policy DEMO7_DB.HRDATA.SSNmask;

// Page 252 - Notice what the ACCOUNTADMIN role sees
USE ROLE ACCOUNTADMIN;
SELECT * FROM DEMO7_DB.HRDATA.EMPLOYEES;

// Page 252 - Notice what Be sure to log in as Adam 
USE ROLE HR_ROLE;
USE WAREHOUSE COMPUTE_WH;
SELECT * FROM DEMO7_DB.HRDATA.EMPLOYEES;

// Page 252 - You should still be logged in as Adam
INSERT INTO DEMO7_DB.HRDATA.EMPLOYEES (emp_id, fname, lname, ssn, email, dept_id, dept) VALUES 
(1, 'Harry', 'Smith', '111-11-1111', 'harry@coemail.com', '100', 'IT'), 
(2, 'Marsha', 'Addison', '222-22-2222', 'marsha@coemail.com', '100','IT'),
(3, 'Javier', 'Sanchez', '333-33-3333', 'javier@coemail.com', '101', 'Marketing'),
(4, 'Alicia', 'Rodriguez', '444-44-4444', 'alicia@coemail.com', '200', 'Finance'),
(5, 'Marco', 'Anderson', '555-55-5555', 'marco@coemail.com', '300', 'HR'),
(6, 'Barbara', 'Francis', '666-66-6666', 'barbara@coemail.com', '400', 'Exec');
SELECT * FROM DEMO7_DB.HRDATA.EMPLOYEES;


// Page 253 - Make sure you are logged back in as yourself
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE masking policy DEMO7_DB.HRDATA.namemask
 AS (EMP_ID integer, DEPT_ID integer) returns integer ->
 CASE WHEN current_role() = 'HR_ROLE'
 then EMP_ID WHEN DEPT_ID = '100'
 then EMP_ID
 ELSE '**MASKED**' END;


// Page 254 - Create a mapping table 
USE ROLE SYSADMIN;
USE DATABASE DEMO7_DB;
USE SCHEMA CH7DATA;
CREATE OR REPLACE TABLE AreaFiltering (role_name text, area_id integer);
INSERT INTO AreaFiltering (role_name, area_id) VALUES 
    ('AREA1', 1), ('AREA2', 2);
SELECT * FROM AreaFiltering;

// Page 254 - Create a secure view 
USE ROLE SYSADMIN;
CREATE OR REPLACE SECURE VIEW V_RATINGS_SUMMARY AS
SELECT emp_id, rating, dept_id, area
FROM RATINGS
WHERE area= (SELECT area_id FROM AreaFiltering
WHERE role_name=CURRENT_ROLE());


// Page 254 - Data can not be seen when using the SYSADMIN role, the current role being used
SELECT * FROM v_ratings_summary;

// Page 254 - Give AREA1_ROLE the ability to see data in the secure view
GRANT SELECT ON ALL TABLES IN SCHEMA CH7DATA TO ROLE AREA1_ROLE;
GRANT SELECT ON AreaFiltering TO ROLE AREA1_ROLE;
GRANT SELECT ON v_ratings_summary TO ROLE AREA1_ROLE;

// Page 254 - Be sure to log in as Adam
USE ROLE AREA1_ROLE;
USE DATABASE DEMO7_DB;
USE SCHEMA CH7DATA;
SELECT * FROM v_ratings_summary;

// Page 255 - Log back in as yourself to run the next statement
USE ROLE SYSADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA CH7DATA TO ROLE AREA2_ROLE;
GRANT SELECT ON AreaFiltering TO ROLE AREA2_ROLE;
GRANT SELECT ON v_ratings_summary TO ROLE AREA2_ROLE;

// Page 255 - Be sure to log in as Adam
USE ROLE AREA2_ROLE;
USE DATABASE DEMO7_DB;
USE SCHEMA CH7DATA;
SELECT * FROM v_ratings_summary;

// Page 255 - Drop the secure view because we want to replace the original view with a secure view 
USE ROLE SYSADMIN;
DROP VIEW v_ratings_summary;

// Page 255 - Create a secure view that uses multicondition query 
USE ROLE SYSADMIN;
USE DATABASE DEMO7_DB;
USE SCHEMA CH7DATA;
CREATE SECURE VIEW v_ratings_summary AS
    SELECT emp_id, rating, dept_id, area
    FROM RATINGS
    WHERE area IN (SELECT area_id FROM AreaFiltering
    WHERE role_name IN 
        (SELECT value FROM TABLE(flatten(input => 
        parse_json(CURRENT_AVAILABLE_ROLES())))) );  

// Page 256 - Give necessary privileges to the roles
GRANT SELECT ON AreaFiltering TO ROLE AREA1_ROLE;
GRANT SELECT ON v_ratings_summary TO ROLE AREA1_ROLE;
GRANT SELECT ON AreaFiltering TO ROLE AREA2_ROLE;
GRANT SELECT ON v_ratings_summary TO ROLE AREA2_ROLE;

// Page 256 - Log in as Adam
USE ROLE AREA1_ROLE;
USE DATABASE DEMO7_DB;
USE SCHEMA CH7DATA;
SELECT * FROM v_ratings_summary;

// Page 256 - Make sure you are still logged in as Adam
USE DATABASE DEMO7_DB;
USE SCHEMA CH7DATA;
SELECT * FROM v_ratings_summary;

// Page 258 - Code Cleanup
USE ROLE ACCOUNTADMIN;
DROP DATABASE DEMO7_DB;
DROP USER ADAM; 