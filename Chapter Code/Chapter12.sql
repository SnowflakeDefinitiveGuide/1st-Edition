// Snowflake Definitive Guide 1st Edition by Joyce Kay Avila - August 2022
// ISBN-10 : 1098103823
// ISBN-13 : 978-1098103828
// Contact the author: https://www.linkedin.com/in/joycekayavila/
// Chapter 12: Workloads for the Snowflake Data Cloud


// Page 372 - Prep Work
// Create new worksheet: Chapter 12 Workloads
// Context setting - make sure role is set to SYSADMIN and COMPUTE_WH is the virtual warehouse
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
CREATE OR REPLACE DATABASE DEMO12_DB;
CREATE OR REPLACE SCHEMA CYBERSECURITY;
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

// Page 392 - Create a base table
CREATE OR REPLACE TABLE BASETABLE AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.LINEITEM;
    
// Page 392 - Clone the table for use in the clustering example and then enable clustering
CREATE OR REPLACE TABLE CLUSTEREXAMPLE CLONE BASETABLE;
ALTER TABLE CLUSTEREXAMPLE CLUSTER BY (L_SHIPDATE); 

// Page 392 - Verify that the clustering is complete
SELECT SYSTEM$CLUSTERING_INFORMATION('CLUSTEREXAMPLE','(L_SHIPDATE)');

// Page 393 - Compare the results to the unclustered base table
SELECT SYSTEM$CLUSTERING_INFORMATION('BASETABLE','(L_SHIPDATE)');

// Page 394 - Clone the table and add Search Optimization
CREATE OR REPLACE TABLE OPTIMIZEEXAMPLE CLONE BASETABLE;
ALTER TABLE OPTIMIZEEXAMPLE ADD SEARCH OPTIMIZATION;

// Page 394 - Use the SHOW command to see the details
SHOW TABLES LIKE '%EXAMPLE%';

// Page 395 - Get an example of a record to be used later for the point lookup query
SELECT * FROM BASETABLE
LIMIT 1;

// Page 396 - Use the previous L_ORDERKEY result value in this query
SELECT * FROM BASETABLE WHERE L_ORDERKEY ='363617027';

// Page 396 - Use the same L_ORDERKEY result value in this query
SELECT * FROM CLUSTEREXAMPLE WHERE L_ORDERKEY ='363617027';

// Page 397 - Use the range of dates from the previous search
SELECT * FROM CLUSTEREXAMPLE
    WHERE L_SHIPDATE >= '1992-12-05'and L_SHIPDATE <='1993-02-20'
    AND L_ORDERKEY = '363617027';

// Page 399 - Use the L_ORDERKEY result value
SELECT * FROM OPTIMIZEEXAMPLE WHERE L_ORDERKEY ='363617027';

// Page 400 - Run the query again now that some time has passed
SELECT SYSTEM$CLUSTERING_INFORMATION('CLUSTEREXAMPLE','(L_SHIPDATE)');

// Page 403 - Code Cleanup
DROP DATABASE DEMO12_DB;
ALTER SESSION SET USE_CACHED_RESULT = TRUE;
