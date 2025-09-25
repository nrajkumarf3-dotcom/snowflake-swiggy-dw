-- Raw/Stage Layer Table and it's corresponding DDL's. 
-- In Swiggy Data Model, we are considering only 10 tables from Source System 

use warehouse adhoc_wh;
use role sysadmin;
use database "sandbox";

use schema "common";
create or replace tag pii_policy_tag
    allowed_values 'PII','PRICE','SENSITIVE','EMAIL'
    comment = 'This is PII Policy Tag Object';