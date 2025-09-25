use ROLE sysadmin;
use warehouse adhoc_wh;
use database "sandbox";
create stage if not exists "stage_sch".stage_csv
    directory = (enable = true)
    comment = 'this is snowfloke internal stage';