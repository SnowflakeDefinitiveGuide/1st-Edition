// Snowflake Definitive Guide 1st Edition by Joyce Kay Avila - August 2022
// ISBN-10 : 1098103823
// ISBN-13 : 978-1098103828
// Contact the author: https://www.linkedin.com/in/joycekayavila/
// Chapter 10: Configuring and Managing Secure Data Sharing


// Page 317 - Prep Work
// Create new worksheet: Chapter10 Data Sharing
// Context setting - make sure role is set to ACCOUNTADMIN and COMPUTE_WH is the virtual warehouse
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
CREATE OR REPLACE DATABASE DEMO10_DB;
USE SCHEMA DEMO10_DB.PUBLIC;
CREATE OR REPLACE TABLE SHARINGDATA (i integer);

// Note -- This file contains the code to be executed in the worksheet
// There are lots of things done in the web UI for this chapter; you can follow the instructions in the book, using the screenshots as your guide


// Page 322 - Create a new share and grant usage on objects
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE SHARE DEMO10_SHARE2;
GRANT USAGE ON DATABASE DEMO10_DB TO SHARE DEMO10_SHARE2;
GRANT USAGE ON SCHEMA DEMO10_DB.PUBLIC TO SHARE DEMO10_SHARE2;
GRANT SELECT ON TABLE DEMO10_DB.PUBLIC.SHARINGDATA TO SHARE DEMO10_SHARE2;

// Page 324 - For use in a later example, create database, schema, and table and insert values
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO10_DB;
CREATE OR REPLACE SCHEMA PRIVATE;
CREATE OR REPLACE TABLE DEMO10_DB.PRIVATE.SENSITIVE_DATA
 (nation string,
 price float,
 size int,
 id string);
INSERT INTO DEMO10_DB.PRIVATE.SENSITIVE_DATA
 values('USA', 123.5, 10,'REGION1'),
 ('USA', 89.2, 14, 'REGION1'),
 ('CAN', 99.0, 35, 'REGION2'),
 ('CAN', 58.0, 22, 'REGION2'),
 ('MEX', 112.6,18, 'REGION2'),
 ('MEX', 144.2,15, 'REGION2'),
 ('IRE', 96.8, 22, 'REGION3'),
 ('IRE', 107.4,19, 'REGION3');
 
// Page 324 - Create a table that will hold the mapping to individual accounts 
CREATE OR REPLACE TABLE DEMO10_DB.PRIVATE.SHARING_ACCESS
(id string,snowflake_account string);
 
// Page 324 - Give current account access to REGION1 data
INSERT INTO SHARING_ACCESS values('REGION1', current_account());
 
 
 // Page 325 - Assign REGION2 and REGION3 the values associated with their respective accounts
INSERT INTO SHARING_ACCESS values('REGION2', 'ACCT2');
INSERT INTO SHARING_ACCESS values('REGION3', 'ACCT3');
SELECT * FROM SHARING_ACCESS;


// Page 325 - Join data in base table with mapping table
CREATE OR REPLACE SECURE VIEW DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA as
 SELECT nation, price, size
 FROM DEMO10_DB.PRIVATE.SENSITIVE_DATA sd
 JOIN DEMO10_DB.PRIVATE.SHARING_ACCESS sa on sd.id = sa.id
 AND sa.snowflake_account = current_account();
 
// Page 325 - Grant SELECT privilege to all roles
GRANT SELECT ON DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA to PUBLIC;

// Page 325 - Look at the data in the Private Sensitive Data table
SELECT * FROM DEMO10_DB.PRIVATE.SENSITIVE_DATA;
 
// Page 326 - Look at the data in the Public Sensitive Data table
SELECT * FROM DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA;
 
// Page 326 - Use a session variable 
ALTER SESSION SET simulated_data_sharing_consumer='ACCT2';
SELECT * FROM DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA;

// Page 327 - Use another session variable
ALTER SESSION SET simulated_data_sharing_consumer='ACCT3';
SELECT * FROM DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA;

// Page 327 - Return our Snowflake account to its original account value
ALTER SESSION UNSET simulated_data_sharing_consumer;

// Page 327 - Create a new share
USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO10_DB;
USE SCHEMA DEMO10_DB.PUBLIC;
CREATE OR REPLACE SHARE NATIONS_SHARED;
SHOW SHARES;

// Page 327 - Grant privileges to the new share
GRANT USAGE ON DATABASE DEMO10_DB TO SHARE NATIONS_SHARED;
GRANT USAGE ON SCHEMA DEMO10_DB.PUBLIC TO SHARE NATIONS_SHARED;
GRANT SELECT ON DEMO10_DB.PUBLIC.PAID_SENSITIVE_DATA TO SHARE NATIONS_SHARED;

// Page 327 - Use SHOW command to confirm contents of the share
SHOW GRANTS TO SHARE NATIONS_SHARED;

// Page 328 - How to add accounts
// In production, you'd need to replace ACCT2 and ACCT4 with actual Snowflake account details
ALTER SHARE NATIONS_SHARED ADD ACCOUNTS = ACCT2, ACCT3;

// Page 328 - Use SHOW command to retrieve list of all Snowflake consumer accounts that have a database created from a share
SHOW GRANTS OF SHARE NATIONS_SHARED;

// Page 345 Code Cleanup
// Error Expected
USE ROLE ACCOUNTADMIN;
DROP DATABASE DEMO10_DB;

// Page 345 - Code Cleanup
REVOKE USAGE ON DATABASE DEMO10_DB FROM SHARE NATIONS_SHARED;
REVOKE USAGE ON DATABASE DEMO10_DB FROM SHARE DEMO10_SHARE;
REVOKE USAGE ON DATABASE DEMO10_DB FROM SHARE DEMO10_SHARE2;
DROP DATABASE DEMO10_DB;