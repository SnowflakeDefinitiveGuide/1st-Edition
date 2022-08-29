// Snowflake Definitive Guide 1st Edition by Joyce Kay Avila - August 2022
// ISBN-10 : 1098103823
// ISBN-13 : 978-1098103828
// Contact the author: https://www.linkedin.com/in/joycekayavila/
// Chapter 11: Visualizing Data in Snowsight


// Page 347 - Prep Work
// Create new folder: Chapter 11
// Create new worksheet: Chapter11 Visualization
// Context setting - make sure role is set to SYSADMIN and COMPUTE_WH is the virtual warehouse

// Page 349 - Set Context
USE ROLE SYSADMIN;
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCDS_SF100TCL;

// Page 349 - There are some examples / instructions that refer to the Snowsight Web UI.  You can find those in the textbook, including screenshots

// Page 352 - Retrieve 100 records from the table
SELECT * FROM STORE_SALES LIMIT 100;

// Page 353 - Retrieve 100 records from the table by using SAMPLE
SELECT * FROM STORE_SALES SAMPLE (100 ROWS);

// Page 353 - Retrieve 100 records from the table by using TABLESAMPLE
SELECT * FROM STORE_SALES TABLESAMPLE SYSTEM (0.015);

// Page 354 - Retrieve 5000000 records
SELECT * FROM CATALOG_RETURNS LIMIT 5000000;

// Page 354 - Retrieve 9999 records
SELECT * FROM CATALOG_RETURNS LIMIT 9999;

// Several exercises are to be done in the web UI, as described in the book

// Page 369 - Code Cleanup
// No cleanup is needed