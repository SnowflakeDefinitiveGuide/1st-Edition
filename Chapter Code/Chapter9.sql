// Snowflake Definitive Guide 1st Edition by Joyce Kay Avila - August 2022
// ISBN-10 : 1098103823
// ISBN-13 : 978-1098103828
// Contact the author: https://www.linkedin.com/in/joycekayavila/
// Chapter 9: Analyzing and Improving Snowflake Query Performance


// Page 287 - Prep Work
// Create new worksheet: Chapter9 Improving Queries
// Context setting - make sure role is set to SYSADMIN and COMPUTE_WH is the virtual warehouse


// Page 288 - Returns details about all queries run by the current user in the past day
// Make sure your role is set to ACCOUNTADMIN and make sure you replace <database name> with the name of the database you want to query
USE ROLE ACCOUNTADMIN;
USE DATABASE <database name>;
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
            dateadd('days', -1, current_timestamp()),
            current_timestamp()))
ORDER BY TOTAL_ELAPSED_TIME DESC;


// Page 288 - Returns information about the queries executed for a particular database, in order of frquency and avg compilation time
// Make sure your role is set to ACCOUNTADMIN and make sure you replace <database name> with the name of the database you want to query
USE ROLE ACCOUNTADMIN;
USE DATABASE <database name>;
SELECT HASH(query_text), QUERY_TEXT, COUNT(*),
    AVG(compilation_time), AVG(execution_time)
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(dateadd('days', -1,
    current_timestamp()),current_timestamp() ) )
GROUP BY HASH(query_text), QUERY_TEXT
ORDER BY COUNT(*) DESC, AVG(compilation_time) DESC ;

// Page 302 - Looks at the clustering information for a specific column
USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF100;
SELECT SYSTEM$CLUSTERING_INFORMATION( 'CUSTOMER' , '(C_NATIONKEY )' );

// Page 303 - Get the distinct count of values as well as total number of records
USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF100;

SELECT COUNT(DISTINCT C_NATIONKEY) FROM CUSTOMER;

SELECT COUNT(C_NATIONKEY) FROM CUSTOMER;

// Page 303 - Calculate the selectivity
USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF100;
SELECT COUNT(DISTINCT C_NATIONKEY) / Count(C_NATIONKEY) FROM CUSTOMER;

// Page 304 - Look at the data distribution as part of considering whether column would be a good candidate for clustering key
SELECT C_NATIONKEY, count(*) FROM CUSTOMER group by C_NATIONKEY;

// Page 305 - Calculate the selectivity
USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF100;
SELECT COUNT(DISTINCT C_NAME) / Count(C_NAME) FROM CUSTOMER;

// Page 305 - Define a clustering key at the time of table creation
// Be sure to replace table name and column name text with an actual table and column
ALTER TABLE <table name> CLUSTER BY (column name(s));

// Page 309 - Add search optimization to a table
// Be sure to replace the table name text with an actual table
ALTER TABLE [IF EXISTS] <table name> ADD SEARCH OPTIMIZATION;

// Page 310 - Code Cleanup
// No cleanup needed since we used the sample database.