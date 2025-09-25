use role sysadmin;

use warehouse adhoc_wh;

use database "sandbox";

create schema IF NOT EXISTS "stage_sch";
create schema IF NOT EXISTS "clean_sch";
create schema IF NOT EXISTS "consumption_sch";
create schema IF NOT EXISTS "common";