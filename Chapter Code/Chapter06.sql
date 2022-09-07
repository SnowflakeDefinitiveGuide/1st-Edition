// Snowflake Definitive Guide 1st Edition by Joyce Kay Avila - August 2022
// ISBN-10 : 1098103823
// ISBN-13 : 978-1098103828
// Contact the author: https://www.linkedin.com/in/joycekayavila/
// Chapter 6: Data Loading and Unloading


// Page 184 - Prep Work
// Create new worksheet: Chapter 12 Workloads
// Context setting - make sure role is set to SYSADMIN and COMPUTE_WH is the virtual warehouse

// Page 184 - Prep Work
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;
CREATE OR REPLACE DATABASE DEMO6_DB
    COMMENT = "Database for all Chapter 6 Examples";
CREATE OR REPLACE SCHEMA WS COMMENT = "Schema for Worksheet Insert Examples";
CREATE OR REPLACE SCHEMA UI COMMENT = "Schema for Web UI Uploads";
CREATE OR REPLACE SCHEMA SNOW COMMENT = "Schema for SnowSQL Loads";
CREATE OR REPLACE WAREHOUSE LOAD_WH
    COMMENT = "Warehouse for CH 6 Load Examples";
 
 
// Page 192 - Create a new database and schema to be used for examples 
USE WAREHOUSE LOAD_WH;
USE DATABASE DEMO6_DB;
USE SCHEMA WS; 

// Page 193 - Create table for single-row inserts for structured data
CREATE OR REPLACE TABLE TABLE1
    (id integer, f_name string, l_name string, city string)
COMMENT = "Single-Row Insert for Structured Data
    using Explicitly Specified Values";
 
// Page 193 - Insert values into the table and confirm there is one row of data in the table 
INSERT INTO TABLE1 (id, f_name, l_name, city)
VALUES (1, 'Anthony', 'Robinson', 'Atlanta');
SELECT * FROM TABLE1;

// Page 193 - Insert another row and confirm
INSERT INTO TABLE1 (id, f_name, l_name, city)
VALUES (2, 'Peggy', 'Mathison', 'Birmingham');
SELECT * FROM TABLE1;

// Page 193 - Create a table to be used for semi-structured datra
CREATE OR REPLACE TABLE TABLE2
    (id integer, variant1 variant)
COMMENT = "Single-Row Insert for Semi-Structured JSON Data";

// Page 193 - Use a query clause to insert values and confirm there is one row of data
INSERT INTO TABLE2 (id, variant1)
SELECT 1, parse_json(' {"f_name": "Anthony", "l_name": "Robinson",
 "city": "Atlanta" } ');
SELECT * FROM TABLE2;

// Page 193 - Insert another row of data
INSERT INTO TABLE2 (id, variant1)
SELECT 2, parse_json(' {"f_name": "Peggy", "l_name": "Mathison",
 "city": "Birmingham" } ');
SELECT * FROM TABLE2;

// Page 194 - Create a new table and insert two rows of data at one time
CREATE OR REPLACE TABLE TABLE3
    (id integer, f_name string, l_name string, city string)
COMMENT = "Multi-row Insert for Structured Data using Explicitly Stated Values";
INSERT INTO TABLE3 (id, f_name, l_name, city) VALUES
    (1, 'Anthony', 'Robinson', 'Atlanta'), (2, 'Peggy', 'Mathison',
    'Birmingham');
SELECT * FROM TABLE3;

// Page 194 - Create a new table and insert only specific records form an existing table
CREATE OR REPLACE TABLE TABLE4
    (id integer, f_name string, l_name string, city string)
COMMENT = "Multi-row Insert for Structured Data using Query, All Columns Same";
INSERT INTO TABLE4 (id, f_name, l_name, city)
    SELECT * FROM TABLE3 WHERE CONTAINS (city, 'Atlanta');
SELECT * FROM TABLE4;

// Page 195 - Create a new table 
CREATE OR REPLACE TABLE TABLE5
    (id integer, f_name string, l_name string, city string)
COMMENT = "Multi-row Insert for Structured Data using Query, Fewer Columns";

// Attempt to insert fewer column values in the new table than from the existing table without specifying columns
// Error is expected
INSERT INTO TABLE5
 (id, f_name, l_name) SELECT * FROM TABLE3 WHERE CONTAINS (city, 'Atlanta');
 
// Page 195 -  Insert fewer column values in the new table than from the existing table
INSERT INTO TABLE5 (id, f_name, l_name)
    SELECT id, f_name, l_name FROM TABLE3 WHERE CONTAINS (city, 'Atlanta');
SELECT * FROM TABLE5;

// Page 196 - Create a table and insert values that will be used in next example
CREATE OR REPLACE TABLE TABLE6
    (id integer, first_name string, last_name string, city_name string)
COMMENT = "Table to be used as part of next demo";

INSERT INTO TABLE6 (id, first_name, last_name, city_name) VALUES
    (1, 'Anthony', 'Robinson', 'Atlanta'),
    (2, 'Peggy', 'Mathison', 'Birmingham');
 
// Page 196 - Create a new table and use CTE 
CREATE OR REPLACE TABLE TABLE7
    (id integer, f_name string, l_name string, city string)
COMMENT = "Multi-row Insert for Structured Data using CTE"; 

INSERT INTO TABLE7 (id, f_name, l_name, city)
    WITH CTE AS
    (SELECT id, first_name as f_name, last_name as l_name,
        city_name as city FROM TABLE6)
    SELECT id, f_name, l_name, city
    FROM CTE;
SELECT * FROM TABLE7;

// Page 196 - Create a new table that will be used in next example
CREATE OR REPLACE TABLE TABLE8
    (id integer, f_name string, l_name string, zip_code string)
COMMENT = "Table to be used as part of next demo";
INSERT INTO TABLE8 (id, f_name, l_name, zip_code)
VALUES (1, 'Anthony', 'Robinson', '30301'), (2, 'Peggy', 'Mathison', '35005');

// Page 196 - Create another table that will be used in the next example
CREATE OR REPLACE TABLE TABLE9
(id integer, zip_code string, city string, state string)
COMMENT = "Table to be used as part of next demo";
INSERT INTO TABLE9 (id, zip_code, city, state) VALUES
    (1, '30301', 'Atlanta', 'Georgia'),
    (2, '35005', 'Birmingham', 'Alabama');
 
// Page 197 - Create a new table to use for inserting records using an ineer join  
CREATE OR REPLACE TABLE TABLE10
    (id integer, f_name string, l_name string, city string,
    state string, zip_code string)
COMMENT = "Multi-row inserts from two tables using an Inner JOIN on zip_code";

INSERT INTO TABLE10 (id, f_name, l_name, city, state, zip_code)
SELECT a.id, a.f_name, a.l_name, b.city, b.state, a.zip_code
FROM TABLE8 a
    INNER JOIN TABLE9 b on a.zip_code = b.zip_code;
SELECT *FROM TABLE10;
 
// Page 197 - Create a new table to be used for semi-structured data 
CREATE OR REPLACE TABLE TABLE11
    (variant1 variant)
COMMENT = "Multi-row Insert for Semi-structured JSON Data";

// Page 197 - Insert values into the table
INSERT INTO TABLE11
 select parse_json(column1)
 from values
 ('{ "_id": "1",
 "name": { "first": "Anthony", "last": "Robinson" },
 "company": "Pascal",
 "email": "anthony@pascal.com",
 "phone": "+1 (999) 444-2222"}'),
 ('{ "id": "2",
 "name": { "first": "Peggy", "last": "Mathison" },
 "company": "Ada",
 "email": "Peggy@ada.com",
 "phone": "+1 (999) 555-3333"}');
SELECT * FROM TABLE11;

// Page 198 - Create a source table and insert values
CREATE OR REPLACE TABLE TABLE12
(id integer, first_name string, last_name string, city_name string)
COMMENT = "Source Table to be used as part of next demo for Unconditional Table
Inserts";
INSERT INTO TABLE12 (id, first_name, last_name, city_name) VALUES
(1, 'Anthony', 'Robinson', 'Atlanta'), (2, 'Peggy', 'Mathison', 'Birmingham');

// Page 198 - Create two target tables
CREATE OR REPLACE TABLE TABLE13
(id integer, f_name string, l_name string, city string)
COMMENT = "Unconditional Table Insert - Destination Table 1 for unconditional
    multi-table insert";

CREATE OR REPLACE TABLE TABLE14
(id integer, f_name string, l_name string, city string)
COMMENT = "Unconditional Table Insert - Destination Table 2 for unconditional
    multi-table insert";

// Page `99 - Use data from Table 12 to insert into two tables -- one insertion for all data and another insertion for select values`
INSERT ALL
    INTO TABLE13
    INTO TABLE13 (id, f_name, l_name, city)
        VALUES (id, last_name, first_name, default)
    INTO TABLE14 (id, f_name, l_name, city)
    INTO TABLE14 VALUES (id, city_name, last_name, first_name)
SELECT id, first_name, last_name, city_name FROM TABLE12;

// Page 199 - Look at data in Table13
SELECT * FROM TABLE13;

// Page 199 - Look at data in Table14
SELECT * FROM TABLE14;

// Page 199 - Create a source table for the next example and insert values
CREATE OR REPLACE TABLE TABLE15
(id integer, first_name string, last_name string, city_name string)
COMMENT = "Source Table to be used as part of next demo for
    Conditional multi-table Insert";
INSERT INTO TABLE15 (id, first_name, last_name, city_name)
VALUES
(1, 'Anthony', 'Robinson', 'Atlanta'),
(2, 'Peggy', 'Mathison', 'Birmingham'),
(3, 'Marshall', 'Baker', 'Chicago'),(4, 'Kevin', 'Cline', 'Denver'),
(5, 'Amy', 'Ranger', 'Everly'),(6, 'Andy', 'Murray', 'Fresno');

// Page 200 Create two target tables
CREATE OR REPLACE TABLE TABLE16
    (id integer, f_name string, l_name string, city string)
COMMENT = "Destination Table 1 for conditional multi-table insert";

CREATE OR REPLACE TABLE TABLE17
    (id integer, f_name string, l_name string, city string)
COMMENT = "Destination Table 2 for conditional multi-table insert";

// Page 200 - Demonstration of a conditional multitable insert
INSERT ALL
    WHEN id <5 THEN
        INTO TABLE16
    WHEN id <3 THEN
        INTO TABLE16
        INTO TABLE17
    WHEN id = 1 THEN
        INTO TABLE16 (id, f_name) VALUES (id, first_name)
    ELSE
        INTO TABLE17
SELECT id, first_name, last_name, city_name FROM TABLE15;

// Page 200 - Look at data in TABLE16
SELECT * FROM TABLE16;

// Page 201 - Look at data in TABLE17
SELECT * FROM TABLE17;

// Page 201 - Create a table to be used for the next example
CREATE OR REPLACE TABLE TABLE18
 (Array variant)
COMMENT = "Insert Array";

// Page 201 - Insert values into TABLE18
INSERT INTO TABLE18
SELECT ARRAY_INSERT(array_construct(0, 1, 2, 3), 4, 4);

// Page 202 - See the data in TABLE18
SELECT * FROM TABLE18;

// Page 202 - Insert values in a different position
INSERT INTO TABLE18
SELECT ARRAY_INSERT(array_construct(0, 1, 2, 3), 7, 4);

// Page 202 - See the data in TABLE18
SELECT * FROM TABLE18;

// Page 202 - Create a new table to be used for next example
CREATE OR REPLACE TABLE TABLE19
    (Object variant)
COMMENT = "Insert Object";

// Page 202 - Insert key-value pairs
INSERT INTO TABLE19
    SELECT OBJECT_INSERT(OBJECT_CONSTRUCT('a', 1, 'b', 2, 'c', 3), 'd', 4);
SELECT * FROM TABLE19;

// Page 202 - Insert values with blank value and different types of null values
INSERT INTO TABLE19 SELECT
    OBJECT_INSERT(object_construct('a', 1, 'b', 2, 'c', 3), 'd', ' ');
INSERT INTO TABLE19 SELECT
    OBJECT_INSERT(object_construct('a', 1, 'b', 2, 'c', 3), 'd', 'null');
INSERT INTO TABLE19 SELECT
    OBJECT_INSERT(object_construct('a', 1, 'b', 2, 'c', 3), 'd', null);
INSERT INTO TABLE19 SELECT
    OBJECT_INSERT(object_construct('a', 1, 'b', 2, 'c', 3), null, 'd');
SELECT * FROM TABLE19;

// Page 204 - Review the table comments
SHOW TABLES LIKE '%TABLE%';

// Page 205 - Create the table to be used for the next example
USE SCHEMA UI;
CREATE OR REPLACE TABLE TABLE20
    (id integer, f_name string, l_name string, city string)
COMMENT = "Load Structured Data file via the Web UI wizard";

// Manually load data as described in the chapter

// Page 210 - Confirm that the data was uploaded
USE DATABASE DEMO6_DB;
USE SCHEMA UI;
SELECT * FROM TABLE20;

// Page 210 Download and install SnowSQL as described in the chapter

// Page 210 - Get signed into SnowSQL
//c:\>snowsql -a dx58224.us-central1.gcp -u JKAVILA2022 <enter>
//Password: ********* <enter>


// Page 210 - In SnowSQL -- Set context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE LOAD_WH;
USE DATABASE DEMO6_DB;
USE SCHEMA SNOW;

// Page 211 -- In SnowSQL - Create a new table
CREATE OR REPLACE TABLE TABLE20 (id integer, f_name string, l_name string, city string);

// Page 211 -- In SnowSQL -- Load the CSV file into the table stage
// Make sure to replace the location in the example with the location on your computer
Put file:///users/joyce/documents/TABLE20.csv @”DEMO6_DB”.”SNOW”.%”TABLE20”;

// Page 211 - In SnowSQL -- Use the COPY INTO command
COPY INTO "TABLE20" FROM @"DEMO6_DB"."SNOW".%"TABLE20" file_format=(type=csv SKIP_HEADER=1);

// Page 222 - Example of how to clone a table
// Make sure you are back in the worksheet
USE ROLE SYSADMIN; USE SCHEMA WS;
CREATE TABLE DEMO_CLONE CLONE TABLE1;

// Page 225 - Code Cleanup
USE WAREHOUSE COMPUTE_WH;
DROP DATABASE DEMO6_DB;
DROP WAREHOUSE LOAD_WH;

















