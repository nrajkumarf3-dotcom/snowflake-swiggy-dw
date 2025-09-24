use role sysadmin;

-- create a warehouse if not exist 
create warehouse if not exists adhoc_wh
     comment = 'This is the adhoc-wh'
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

--Create Schema and Database 

use ROLE sysadmin;

use warehouse adhoc_wh;

--create database 
create database "sandbox";

use database "sandbox";

create schema IF NOT EXISTS "stage_sch";
create schema IF NOT EXISTS "clean_sch";
create schema IF NOT EXISTS "consumption_sch";
create schema IF NOT EXISTS "common";

ALTER SESSION SET SEARCH_PATH = '$current, "clean_sch", "stage_sch","consumption_sch","common"';


use schema "stage_sch";

create file format if not exists csv_file_format
    type = 'csv'
    compression = 'auto'
    field_delimiter = ','
    record_delimiter = '\n'
    skip_header = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    null_if = ('\\N');

create stage stage_csv
    directory = (enable = true)
    comment = 'this is snowfloke internal stage';

use schema "common";
create or replace tag pii_policy_tag
    allowed_values 'PII', 'PRICE','SENSITIVE','EMAIL'
    comment = 'This is PII Policy Tag Object';

create or replace masking policy pii_masking_policy
    as (pii_text string) 
    returns string ->
    to_varchar('** PII **');

create or replace masking policy email_masking_policy
    as (email_text string) 
    returns string ->
    to_varchar('** EMAIL **');    

create or replace masking policy phone_masking_policy
    as (phone_text string) 
    returns string ->
    to_varchar('** Phone **');    

use schema "stage_sch";


use warehouse adhoc_wh;
list @stage_csv;

select $1, $2, $3, $4, $5 from @stage_csv/full-load/location (file_format => 'csv_file_format') limit 10;