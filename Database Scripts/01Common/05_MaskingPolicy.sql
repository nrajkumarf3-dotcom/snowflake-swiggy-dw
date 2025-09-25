-- Raw/Stage Layer Table and it's corresponding DDL's. 
-- In Swiggy Data Model, we are considering only 10 tables from Source System 

use warehouse adhoc_wh;
use role sysadmin;

use database "sandbox";

create or replace masking policy "common"."pii_masking_policy"
    as (pii_text string) 
    returns string ->
    to_varchar('** PII **');

create or replace masking policy "common"."email_masking_policy"
    as (email_text string) 
    returns string ->
    to_varchar('** EMAIL **');    

create or replace masking policy "common"."phone_masking_policy"
    as (phone_text string) 
    returns string ->
    to_varchar('** Phone **');    