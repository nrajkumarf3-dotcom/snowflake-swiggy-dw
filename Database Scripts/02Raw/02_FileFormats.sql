-- Raw/Stage Layer Table and it's corresponding DDL's. 
-- In Swiggy Data Model, we are considering only 10 tables from Source System 

use warehouse adhoc_wh;
use role sysadmin;
use database "sandbox";
create file format if not exists "stage_sch".csv_file_format
    type = 'csv'
    compression = 'auto'
    field_delimiter = ','
    record_delimiter = '\n'
    skip_header = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    null_if = ('\\N');