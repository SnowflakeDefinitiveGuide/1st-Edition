// Snowflake Definitive Guide 1st Edition by Joyce Kay Avila - August 2022
// ISBN-10 : 1098103823
// ISBN-13 : 978-1098103828
// Contact the author: https://www.linkedin.com/in/joycekayavila/
// Chapter 1: Getting Started


// Page 3 - Prep Work
// Set up trial account if needed, see Appendix C
// Navigate to the Snowsight UI, if Classic Console is the default

// Page 6 - Set your role 
// Set your role to SYSADMIN

// Page 7 - Profile submenu
// Review the Profile submenu and enroll in MFA, if desired

// Page 9 - Context setting
// Make sure your role is set to SYSADMIN and your warehouse to COMPUTE_WH

// Page 9 - Create folder and worksheet
// Create folder: Chapter 1
// Create worksheet: Chapter1 Getting Started

// Page 11 - Using the blue arrow button, the Run button in Snowflake
SELECT CURRENT_ROLE();
SELECT CURRENT_WAREHOUSE();

// Page 12 - See what is the current database
SELECT CURRENT_DATABASE();

// Page 12 - Set context for the database to be used
USE DATABASE SNOWFLAKE_SAMPLE_DATA;

// Page 13 - Set context for the schema to be used
// Select the TPCDS_SF100TCL Schema from the menu

// Page 14 - Improved Productivity 
// Reivew the Smart Autocomplete, Format Query, Shortcuts, and Query History

// Page 17 - Click on the House icon to return to the main menu

// Page 20 - Review the "Naming Standards" section

// Page 21 - Code Cleanup
// No code cleanup needed for this chapter
