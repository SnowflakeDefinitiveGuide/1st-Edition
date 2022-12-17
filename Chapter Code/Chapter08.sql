// Snowflake Definitive Guide 1st Edition by Joyce Kay Avila - August 2022
// ISBN-10 : 1098103823
// ISBN-13 : 978-1098103828
// Contact the author: https://www.linkedin.com/in/joycekayavila/
// Chapter 8: Managing Snowflake Account Costs


// Page 262 - Prep Work
// Create new worksheet: Chapter8 Managing Costs
// Context setting - make sure role is set to SYSADMIN and COMPUTE_WH is the virtual warehouse

// Page 262 - Create virtual warehouses to be used for resource monitor section
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
CREATE OR REPLACE WAREHOUSE VW2_WH WITH WAREHOUSE_SIZE = MEDIUM
 AUTO_SUSPEND = 300 AUTO_RESUME = true, INITIALLY_SUSPENDED=true;
CREATE OR REPLACE WAREHOUSE VW3_WH WITH WAREHOUSE_SIZE = SMALL
 AUTO_SUSPEND = 300 AUTO_RESUME = true, INITIALLY_SUSPENDED=true;
CREATE OR REPLACE WAREHOUSE VW4_WH WITH WAREHOUSE_SIZE = MEDIUM
 AUTO_SUSPEND = 300 AUTO_RESUME = true, INITIALLY_SUSPENDED=true;
CREATE OR REPLACE WAREHOUSE VW5_WH WITH WAREHOUSE_SIZE = SMALL
 AUTO_SUSPEND = 300 AUTO_RESUME = true, INITIALLY_SUSPENDED=true;
CREATE OR REPLACE WAREHOUSE VW6_WH WITH WAREHOUSE_SIZE = MEDIUM
 AUTO_SUSPEND = 300 AUTO_RESUME = true, INITIALLY_SUSPENDED=true;
 
// Page 262 - Use the Home icon to navigate to the Main Menu
// Make sure your role is set to ACCOUNTADMIN

// Page 263 - Navigate to Admin --> Usage

// Page 272 - Create an account-level resource monitor
// Make sure you have navigated back to the worksheet and that your role is set to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR MONITOR1_RM WITH CREDIT_QUOTA = 5000
    TRIGGERS on 50 percent do notify
             on 75 percent do notify
             on 100 percent do notify
             on 110 percent do notify
             on 125 percent do notify;
 
// Page 272 - Assing the account-level resource monitor to our account
USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET RESOURCE_MONITOR = MONITOR1_RM;

// Page 272 - Create a virtual warehouse-level monitor and assign it to the priority resource monitor
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR MONITOR5_RM WITH CREDIT_QUOTA = 1500
    TRIGGERS on 50 percent do notify
             on 75 percent do notify
             on 100 percent do notify
             on 110 percent do notify
             on 125 percent do notify;
ALTER WAREHOUSE VW2_WH SET RESOURCE_MONITOR = MONITOR5_RM;

// Page 272 - Look at the resource monitors created thus far
USE ROLE ACCOUNTADMIN;
SHOW RESOURCE MONITORS;

// Page 273 - Create another warehouse-level resource monitor
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR MONITOR2_RM WITH CREDIT_QUOTA = 500
    TRIGGERS on 50 percent do notify
             on 75 percent do notify
             on 100 percent do notify
             on 100 percent do suspend
             on 110 percent do notify
             on 110 percent do suspend_immediate;
ALTER WAREHOUSE VW3_WH SET RESOURCE_MONITOR = MONITOR2_RM;


// Page 273 Create another resource monitor and assign it
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR MONITOR6_RM WITH CREDIT_QUOTA = 500
    TRIGGERS on 50 percent do notify
             on 75 percent do notify
             on 100 percent do notify
             on 100 percent do suspend
             on 110 percent do notify
             on 110 percent do suspend_immediate;
ALTER WAREHOUSE VW6_WH SET RESOURCE_MONITOR = MONITOR6_RM;


// Create another resource monitor and assign it
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR MONITOR4_RM WITH CREDIT_QUOTA = 500
    TRIGGERS on 50 percent do notify
             on 75 percent do notify
             on 100 percent do notify
             on 100 percent do suspend
             on 110 percent do notify
             on 110 percent do suspend_immediate;
ALTER WAREHOUSE VW6_WH SET RESOURCE_MONITOR = MONITOR4_RM;

// Page 274 - Look at the details for all resource monitors
USE ROLE ACCOUNTADMIN;
SHOW RESOURCE MONITORS;

// Page 274 - Look at the details for all virtual warehouses
USE ROLE ACCOUNTADMIN;
SHOW WAREHOUSES;

// Page 274 - Drop the resource monitor we created in error
DROP RESOURCE MONITOR MONITOR6_RM;

// Page 275 - Create the final resource monitor and assign to two virtual warehouses
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR MONITOR3_RM WITH CREDIT_QUOTA = 1500
    TRIGGERS on 50 percent do notify
             on 75 percent do notify
             on 100 percent do notify
             on 100 percent do suspend
             on 110 percent do notify
             on 110 percent do suspend_immediate;
ALTER WAREHOUSE VW4_WH SET RESOURCE_MONITOR = MONITOR3_RM;
ALTER WAREHOUSE VW5_WH SET RESOURCE_MONITOR = MONITOR3_RM;

// Page 275 - Look at the details for all resource monitors
USE ROLE ACCOUNTADMIN;
SHOW RESOURCE MONITORS;

// Page 275 - Look at the details for all virtual warehouses
SHOW WAREHOUSES;

// Page 275  Get details about the virtual warehouse used in the last query
// Make sure you have run the "SHOW WAREHOUSES;" statement just prior to running this code
SELECT "name", "size"
FROM TABLE (RESULT_SCAN(LAST_QUERY_ID()))
WHERE "resource_monitor" = 'null';

// Page 277 - Provides details of virtual warehouse cost by start time, assuming $3 is the credit price
USE ROLE ACCOUNTADMIN;
SET CREDIT_PRICE = 3.00;
USE DATABASE SNOWFLAKE;
USE SCHEMA ACCOUNT_USAGE;
SELECT WAREHOUSE_NAME, START_TIME, END_TIME, CREDITS_USED,
    ($CREDIT_PRICE*CREDITS_USED) AS DOLLARS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
ORDER BY START_TIME DESC;

// Page 277 - Provides details of virtual warehouses cost for past 30 days, assuming $3 is the credit price
SELECT WAREHOUSE_NAME,SUM(CREDITS_USED_COMPUTE)
    AS CREDITS_USED_COMPUTE_30DAYS,
    ($CREDIT_PRICE*CREDITS_USED_COMPUTE_30DAYS) AS DOLLARS_USED_30DAYS
FROM ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;

// Page 282 - Create roles for specific prod, dev, and test environments
USE ROLE SECURITYADMIN;
CREATE OR REPLACE ROLE PROD_ADMIN;
CREATE OR REPLACE ROLE DEV_ADMIN;
CREATE OR REPLACE ROLE QA_ADMIN;
GRANT ROLE PROD_ADMIN TO ROLE SYSADMIN;
GRANT ROLE DEV_ADMIN TO ROLE SYSADMIN;
GRANT ROLE QA_ADMIN TO ROLE SYSADMIN;

// Page 282 - Grant privileges to the new roles
USE ROLE SYSADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE PROD_ADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE DEV_ADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE QA_ADMIN;

// Page 282 - Rename virtual warehouses
USE ROLE SYSADMIN;
ALTER WAREHOUSE IF EXISTS VW2_WH RENAME TO WH_PROD;
ALTER WAREHOUSE IF EXISTS VW3_WH RENAME TO WH_DEV;
ALTER WAREHOUSE IF EXISTS VW4_WH RENAME TO WH_QA;
SHOW WAREHOUSES;

// Page 282 - Grant usage to the associates roles 
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
GRANT USAGE ON WAREHOUSE WH_PROD TO ROLE PROD_ADMIN;
GRANT USAGE ON WAREHOUSE WH_DEV TO ROLE DEV_ADMIN;
GRANT USAGE ON WAREHOUSE WH_QA TO ROLE QA_ADMIN;

// Page 282 - Create prod database, schema, and table and grant usage to Prod Administrator Role
USE ROLE PROD_ADMIN; USE WAREHOUSE WH_PROD;
CREATE OR REPLACE DATABASE PROD_DB;
CREATE OR REPLACE SCHEMA CH8_SCHEMA;
CREATE OR REPLACE TABLE TABLE_A
 (Customer_Account int, Amount int, transaction_ts timestamp);
GRANT USAGE ON DATABASE PROD_DB TO ROLE DEV_ADMIN;

// Page 282 - Create a clone of dev dataabase and grant usage to QA Administrator Role
USE ROLE DEV_ADMIN;
USE WAREHOUSE WH_DEV;
CREATE OR REPLACE DATABASE DEV_DB CLONE PROD_DB;
GRANT USAGE ON DATABASE DEV_DB TO ROLE QA_ADMIN;

// Page 283 - Create a clone of QA database and grant use to Prod Administrator Role
USE ROLE QA_ADMIN;
USE WAREHOUSE WH_QA;
CREATE OR REPLACE DATABASE QA_DB CLONE DEV_DB;
GRANT USAGE ON DATABASE QA_DB TO ROLE PROD_ADMIN;

// Page 283 - Create a new development schema and a new table
USE ROLE DEV_ADMIN; USE WAREHOUSE WH_DEV; USE DATABASE DEV_DB;
CREATE OR REPLACE SCHEMA DEVELOPMENT;
CREATE OR REPLACE TABLE TABLE_B
    (Vendor_Account int, Amount int, transaction_ts timestamp);
GRANT USAGE ON SCHEMA DEVELOPMENT TO ROLE QA_ADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA DEVELOPMENT TO ROLE QA_ADMIN;

// Page 283 QA can conduct testing 
USE ROLE QA_ADMIN; USE WAREHOUSE WH_QA; USE DATABASE QA_DB;
CREATE OR REPLACE SCHEMA TEST;
CREATE OR REPLACE TABLE QA_DB.TEST.TABLE_B
 AS SELECT * FROM DEV_DB.DEVELOPMENT.TABLE_B;
GRANT USAGE ON SCHEMA TEST TO ROLE PROD_ADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA TEST TO ROLE PROD_ADMIN;

// Page 283 - Prod Admin can now copy the table into production
USE ROLE PROD_ADMIN;
USE WAREHOUSE WH_PROD;
USE DATABASE PROD_DB;
USE SCHEMA CH8_SCHEMA;
CREATE OR REPLACE TABLE TABLE_B AS SELECT * FROM QA_DB.TEST.TABLE_B;

// Page 284 Code Cleanup
USE ROLE ACCOUNTADMIN;
DROP DATABASE DEV_DB; DROP DATABASE PROD_DB; DROP DATABASE QA_DB;
DROP ROLE PROD_ADMIN; DROP ROLE DEV_ADMIN; DROP ROLE QA_ADMIN;

DROP RESOURCE MONITOR MONITOR1_RM; DROP RESOURCE MONITOR MONITOR2_RM;
DROP RESOURCE MONITOR MONITOR3_RM; DROP RESOURCE MONITOR MONITOR4_RM;
DROP RESOURCE MONITOR MONITOR5_RM;

DROP WAREHOUSE WH_PROD; DROP WAREHOUSE WH_DEV; DROP WAREHOUSE WH_QA;
DROP WAREHOUSE VW5_WH; DROP WAREHOUSE VW6_WH;


