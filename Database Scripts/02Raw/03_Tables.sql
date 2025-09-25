-- Raw/Stage Layer Table and it's corresponding DDL's. 
-- In Swiggy Data Model, we are considering only 10 tables from Source System 

use warehouse adhoc_wh;
use role sysadmin;

use database "sandbox";

create table if not exists "stage_sch".location (
    locationid text,
    city text,
    state text,
    zipcode text,
    activeflag text,
    createddate text,
    modifieddate text,
    -- audit columns for tracking & debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the location stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;

create table if not exists "stage_sch".restaurant (
    restaurantid text,      
    name text ,                                         -- restaurant name, required field
    cuisinetype text,                                    -- type of cuisine offered
    pricing_for_2 text,                                  -- pricing for two people as text
    restaurant_phone text WITH TAG ("common".pii_policy_tag = 'SENSITIVE'),                               -- phone number as text
    operatinghours text,                                 -- restaurant operating hours
    locationid text ,                                    -- location id, default as text
    activeflag text ,                                    -- active status
    openstatus text ,                                    -- open status
    locality text,                                       -- locality as text
    restaurant_address text,                             -- address as text
    latitude text,                                       -- latitude as text for precision
    longitude text,                                      -- longitude as text for precision
    createddate text,                                    -- record creation date
    modifieddate text,                                   -- last modified date

    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the restaurant stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;

create table if not exists "stage_sch".customer (
    customerid text,                    -- primary key as text
    name text,                          -- name as text
    mobile text WITH TAG ("common".pii_policy_tag = 'PII'),                        -- mobile number as text
    email text WITH TAG ("common".pii_policy_tag = 'EMAIL'),                         -- email as text
    loginbyusing text,                  -- login method as text
    gender text WITH TAG ("common".pii_policy_tag = 'PII'),                        -- gender as text
    dob text WITH TAG ("common".pii_policy_tag = 'PII'),                           -- date of birth as text
    anniversary text,                   -- anniversary as text
    preferences text,                   -- preferences as text
    createddate text,                   -- created date as text
    modifieddate text,                  -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the customer stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create table if not exists "stage_sch".customeraddress (
    addressid text,                    -- primary key as text
    customerid text comment 'Customer FK (Source Data)',                   -- foreign key reference as text (no constraint in snowflake)
    flatno text,                       -- flat number as text
    houseno text,                      -- house number as text
    floor text,                        -- floor as text
    building text,                     -- building name as text
    landmark text,                     -- landmark as text
    locality text,                     -- locality as text
    city text,                          -- city as text
    state text,                         -- state as text
    pincode text,                       -- pincode as text
    coordinates text,                  -- coordinates as text
    primaryflag text,                  -- primary flag as text
    addresstype text,                  -- address type as text
    createddate text,                  -- created date as text
    modifieddate text,                 -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the customer address stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create table if not exists "stage_sch".deliveryagent (
    deliveryagentid text comment 'Primary Key (Source System)',         -- primary key as text
    name text,           -- name as text, required field
    phone text,            -- phone as text, unique constraint indicated
    vehicletype text,             -- vehicle type as text
    locationid text,              -- foreign key reference as text (no constraint in snowflake)
    status text,                  -- status as text
    gender text,                  -- status as text
    rating text,                  -- rating as text
    createddate text,             -- created date as text
    modifieddate text,            -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the delivery stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create table if not exists "stage_sch".menu (
    menuid text comment 'Primary Key (Source System)',                   -- primary key as text
    restaurantid text comment 'Restaurant FK(Source System)',             -- foreign key reference as text (no constraint in snowflake)
    itemname text,                 -- item name as text
    description text,              -- description as text
    price text,                    -- price as text (no decimal constraint)
    category text,                 -- category as text
    availability text,             -- availability as text
    itemtype text,                 -- item type as text
    createddate text,              -- created date as text
    modifieddate text,             -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the menu stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create table if not exists "stage_sch".delivery (
    deliveryid text comment 'Primary Key (Source System)',                           -- foreign key reference as text (no constraint in snowflake)
    orderid text comment 'Order FK (Source System)',                           -- foreign key reference as text (no constraint in snowflake)
    deliveryagentid text comment 'Delivery Agent FK(Source System)',                   -- foreign key reference as text (no constraint in snowflake)
    deliverystatus text,                    -- delivery status as text
    estimatedtime text,                     -- estimated time as text
    addressid text comment 'Customer Address FK(Source System)',                         -- foreign key reference as text (no constraint in snowflake)
    deliverydate text,                      -- delivery date as text
    createddate text,                       -- created date as text
    modifieddate text,                      -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the delivery stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create table if not exists "stage_sch".orders (
    orderid text comment 'Primary Key (Source System)',                  -- primary key as text
    customerid text comment 'Customer FK(Source System)',               -- foreign key reference as text (no constraint in snowflake)
    restaurantid text comment 'Restaurant FK(Source System)',             -- foreign key reference as text (no constraint in snowflake)
    orderdate text,                -- order date as text
    totalamount text,              -- total amount as text (no decimal constraint)
    status text,                   -- status as text
    paymentmethod text,            -- payment method as text
    createddate text,              -- created date as text
    modifieddate text,             -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the order stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

create table if not exists "stage_sch".orderitem (
    orderitemid text comment 'Primary Key (Source System)',              -- primary key as text
    orderid text comment 'Order FK(Source System)',                  -- foreign key reference as text (no constraint in snowflake)
    menuid text comment 'Menu FK(Source System)',                   -- foreign key reference as text (no constraint in snowflake)
    quantity text,                 -- quantity as text
    price text,                    -- price as text (no decimal constraint)
    subtotal text,                 -- subtotal as text (no decimal constraint)
    createddate text,              -- created date as text
    modifieddate text,             -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the order item stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';

