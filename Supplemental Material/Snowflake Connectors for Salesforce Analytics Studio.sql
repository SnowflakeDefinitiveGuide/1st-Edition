//Youtube Video:

--My Youtube Channel: youtube.com/c/joycekayavila

--Link to the specific video associated with this tutorial:   https://youtu.be/UYFXIwRBB1I



-----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------


//PREWORK

//Note: for simplicity, the ACCOUNTADMIN and SYSADMIN roles will be used for this tutorial as well as the CREATE OR REPLACE option
--but this would not likely be the case in a real production environment, especially for service accounts.  There are several best
--practices that we're not following in this example, for the sake of being able to quickly demonstrate the main topic.

//Set Context
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

//Create Virtual Warehouses
--for the Service Accounts
--and grant usage to SYSADMIN
CREATE OR REPLACE WAREHOUSE SVC1_WH WITH WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 300 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
GRANT USAGE ON WAREHOUSE SVC1_WH TO ROLE SYSADMIN;
CREATE OR REPLACE WAREHOUSE SVC2_WH WITH WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 300 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
GRANT USAGE ON WAREHOUSE SVC2_WH TO ROLE SYSADMIN;
CREATE OR REPLACE WAREHOUSE SVC3_WH WITH WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 300 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
GRANT USAGE ON WAREHOUSE SVC3_WH TO ROLE SYSADMIN;
CREATE OR REPLACE WAREHOUSE SVC4_WH WITH WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 300 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
GRANT USAGE ON WAREHOUSE SVC4_WH TO ROLE SYSADMIN;


//Set Context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

//Create Service Account Users with Defaults
--Example shown for one Service Account User
--Create four service account users in total
CREATE OR REPLACE USER SVC1_USER
PASSWORD='123'
MUST_CHANGE_PASSWORD=FALSE;
GRANT ROLE ACCOUNTADMIN TO USER SVC1_USER;
ALTER USER SVC1_USER SET DEFAULT_WAREHOUSE = SVC1_WH;
ALTER USER SVC1_USER SET DEFAULT_ROLE = SYSADMIN;

CREATE OR REPLACE USER SVC2_USER
PASSWORD='123'
MUST_CHANGE_PASSWORD=FALSE;
GRANT ROLE ACCOUNTADMIN TO USER SVC2_USER;
ALTER USER SVC2_USER SET DEFAULT_WAREHOUSE = SVC2_WH;
ALTER USER SVC2_USER SET DEFAULT_ROLE = SYSADMIN;

CREATE OR REPLACE USER SVC3_USER
PASSWORD='123'
MUST_CHANGE_PASSWORD=FALSE;
GRANT ROLE ACCOUNTADMIN TO USER SVC3_USER;
ALTER USER SVC3_USER SET DEFAULT_WAREHOUSE = SVC3_WH;
ALTER USER SVC3_USER SET DEFAULT_ROLE = SYSADMIN;

CREATE OR REPLACE USER SVC4_USER
PASSWORD='123'
MUST_CHANGE_PASSWORD=FALSE;
GRANT ROLE ACCOUNTADMIN TO USER SVC4_USER;
ALTER USER SVC4_USER SET DEFAULT_WAREHOUSE = SVC4_WH;
ALTER USER SVC4_USER SET DEFAULT_ROLE = SYSADMIN;


//Set Context
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

//Create database and four schemas
CREATE OR REPLACE DATABASE SFDC;
CREATE OR REPLACE SCHEMA SFDC.SCHEMA1;
CREATE OR REPLACE SCHEMA SFDC.SCHEMA2;
CREATE OR REPLACE SCHEMA SFDC.SCHEMA3;
CREATE OR REPLACE SCHEMA SFDC.SCHEMA4;

//Create one table in each new schema
--Example shown for one table
CREATE OR REPLACE TABLE SFDC.SCHEMA1.TABLE1
    (id integer, f_name string, l_name string, zip_code string);
INSERT INTO SFDC.SCHEMA1.TABLE1 (id, f_name, l_name, zip_code) VALUES
    (1, 'Arvind', 'Adams', 30301), (2, 'Patricia', 'Barnes', '35005');


CREATE OR REPLACE TABLE SFDC.SCHEMA2.TABLE1
    (id integer, f_name string, l_name string, zip_code string);
INSERT INTO SFDC.SCHEMA2.TABLE1 (id, f_name, l_name, zip_code) VALUES
    (1, 'Arvind', 'Adams', 30301), (2, 'Patricia', 'Barnes', '35005');


CREATE OR REPLACE TABLE SFDC.SCHEMA3.TABLE1
    (id integer, f_name string, l_name string, zip_code string);
INSERT INTO SFDC.SCHEMA3.TABLE1 (id, f_name, l_name, zip_code) VALUES
    (1, 'Arvind', 'Adams', 30301), (2, 'Patricia', 'Barnes', '35005');


CREATE OR REPLACE TABLE SFDC.SCHEMA4.TABLE1
    (id integer, f_name string, l_name string, zip_code string);
INSERT INTO SFDC.SCHEMA1.TABLE1 (id, f_name, l_name, zip_code) VALUES
    (1, 'Arvind', 'Adams', 30301), (2, 'Patricia', 'Barnes', '35005');



//Set Context
USE ROLE SYSADMIN;
USE WAREHOUSE COMPUTE_WH;

//Insert more values into Schema2 Table1
INSERT INTO SFDC.SCHEMA2.TABLE1 (id, f_name, l_name, zip_code) VALUES
    (3, 'Bobby', 'Carrol', '76012'), (4, 'Eugene', 'Davis', '35005');





//Cleanup
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

DROP USER SVC1_USER;
DROP USER SVC2_USER;
DROP USER SVC3_USER;
DROP USER SVC4_USER;

DROP WAREHOUSE SVC1_WH;
DROP WAREHOUSE SVC2_WH;
DROP WAREHOUSE SVC3_WH;
DROP WAREHOUSE SVC4_WH;

DROP DATABASE SFDC;