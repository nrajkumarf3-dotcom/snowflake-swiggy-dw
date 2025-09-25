-- Raw/Stage Layer Table and it's corresponding DDL's. 
-- In Swiggy Data Model, we are considering only 10 tables from Source System 

use warehouse adhoc_wh;
use role sysadmin;

use database "sandbox";

create table if not exists "clean_sch".restaurant_location (
    restaurant_location_sk number autoincrement primary key,
    location_id number not null unique,
    city string(100) not null,
    state string(100) not null,
    state_code string(2) not null,
    is_union_territory boolean not null default false,
    capital_city_flag boolean not null default false,
    city_tier text(6),
    zip_code string(10) not null,
    active_flag string(10) not null,
    created_ts timestamp_tz not null,
    modified_ts timestamp_tz,
    
    -- additional audit columns
    _stg_file_name string,
    _stg_file_load_ts timestamp_ntz,
    _stg_file_md5 string,
    _copy_data_ts timestamp_ntz default current_timestamp
)
comment = 'Location entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create table if not exists "clean_sch".restaurant (
    restaurant_sk number autoincrement primary key,              -- primary key with auto-increment
    restaurant_id number unique,                                        -- restaurant id without auto-increment
    name string(100) not null,                                   -- restaurant name, required field
    cuisine_type string,                                         -- type of cuisine offered
    pricing_for_two number(10, 2),                               -- pricing for two people, up to 10 digits with 2 decimal places
    restaurant_phone string(15) WITH TAG ("common".pii_policy_tag = 'SENSITIVE'),                                 -- phone number, supports 10-digit or international format
    operating_hours string(100),                                  -- restaurant operating hours
    location_id_fk number,                                       -- reference id for location, defaulted to 1
    active_flag string(10),                                      -- indicates if the restaurant is active
    open_status string(10),                                      -- indicates if the restaurant is currently open
    locality string(100),                                        -- locality of the restaurant
    restaurant_address string,                                   -- address of the restaurant, supports longer text
    latitude number(9, 6),                                       -- latitude with 6 decimal places for precision
    longitude number(9, 6),                                      -- longitude with 6 decimal places for precision
    created_dt timestamp_tz,                                     -- record creation date
    modified_dt timestamp_tz,                                    -- last modified date, allows null if not modified

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp for audit
    _stg_file_md5 string,                                        -- md5 hash for file content for audit
    _copy_data_ts timestamp_ntz default current_timestamp        -- timestamp when data is copied, defaults to current timestamp
)
comment = 'Restaurant entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create table if not exists "clean_sch".customer (
    
    customer_sk number autoincrement primary key,                -- auto-incremented primary key
    customer_id string not null,                                 -- customer id
    name string(100) not null,                                   -- customer name
    mobile string(15)  with tag ("common".pii_policy_tag = 'PII'),   -- mobile number, accommodating international format
    email string(100) with tag ("common".pii_policy_tag = 'EMAIL'),  -- email
    login_by_using string(50),                                   -- method of login (e.g., social, google, etc.)
    gender string(10)  with tag ("common".pii_policy_tag = 'PII'), -- gender
    dob date WITH tag ("common".pii_policy_tag = 'PII'),         -- date of birth in date format
    anniversary date,                                            -- anniversary in date format
    preferences string,                                          -- customer preferences
    created_dt timestamp_tz default current_timestamp,           -- record creation timestamp
    modified_dt timestamp_tz,                                    -- record modification timestamp, allows null if not modified

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp
    _stg_file_md5 string,                                        -- md5 hash for file content
    _copy_data_ts timestamp_ntz default current_timestamp        -- copy data timestamp
)
comment = 'Customer entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create table if not exists "clean_sch".customer_address (
    customer_address_sk number autoincrement primary key comment 'surrogate key (ewh)',                -- auto-incremented primary key
    address_id int comment 'primary key (source data)',                 -- primary key as string
    customer_id_fk int comment 'customer fk (source data)',                -- foreign key reference as string (no constraint in snowflake)
    flat_no string,                    -- flat number as string
    house_no string,                   -- house number as string
    floor string,                      -- floor as string
    building string,                   -- building name as string
    landmark string,                   -- landmark as string
    locality string,                   -- locality as string
    city string,                       -- city as string
    state string,                      -- state as string
    pincode string,                    -- pincode as string
    coordinates string,                -- coordinates as string
    primary_flag string,               -- primary flag as string
    address_type string,               -- address type as string
    created_date timestamp_tz,         -- created date as timestamp with time zone
    modified_date timestamp_tz,        -- modified date as timestamp with time zone

    -- audit columns with appropriate data types
    _stg_file_name string,
    _stg_file_load_ts timestamp,
    _stg_file_md5 string,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'customer address entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. this table does not support scd2';

create table if not exists "clean_sch".menu (
    menu_sk int autoincrement primary key comment 'surrogate key (edw)',  -- auto-incrementing primary key for internal tracking
    menu_id int not null unique comment 'primary key (source system)' ,             -- unique and non-null menu_id
    restaurant_id_fk int comment 'restaurant fk(source system)' ,                      -- identifier for the restaurant
    item_name string not null,                        -- name of the menu item
    description string not null,                     -- description of the menu item
    price decimal(10, 2) not null,                   -- price as a numeric value with 2 decimal places
    category string,                        -- food category (e.g., north indian)
    availability boolean,                   -- availability status (true/false)
    item_type string,                        -- dietary classification (e.g., vegan)
    created_dt timestamp_ntz,               -- date when the record was created
    modified_dt timestamp_ntz,              -- date when the record was last modified

    -- audit columns for traceability
    _stg_file_name string,                  -- source file name
    _stg_file_load_ts timestamp_ntz,        -- timestamp when data was loaded from the staging layer
    _stg_file_md5 string,                   -- md5 hash of the source file
    _copy_data_ts timestamp_ntz default current_timestamp -- timestamp when data was copied to the clean layer
)
comment = 'menu entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. this table does not support scd2';

CREATE TABLE if not exists "clean_sch".delivery_agent (
    delivery_agent_sk INT AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)', -- Primary key with auto-increment
    delivery_agent_id INT NOT NULL UNIQUE comment 'Primary Key (Source System)',               -- Delivery agent ID as integer
    name STRING NOT NULL,                -- Name as string, required field
    phone STRING NOT NULL,                 -- Phone as string, unique constraint
    vehicle_type STRING NOT NULL,                 -- Vehicle type as string
    location_id_fk INT comment 'Location FK(Source System)',                     -- Location ID as integer
    status STRING,                       -- Status as string
    gender STRING,                       -- Gender as string
    rating number(4,2),                        -- Rating as float
    created_dt TIMESTAMP_NTZ,          -- Created date as timestamp without timezone
    modified_dt TIMESTAMP_NTZ,         -- Modified date as timestamp without timezone

    -- Audit columns with appropriate data types
    _stg_file_name STRING,               -- Staging file name as string
    _stg_file_load_ts TIMESTAMP,         -- Staging file load timestamp
    _stg_file_md5 STRING,                -- Staging file MD5 hash as string
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Data copy timestamp with default value
)
comment = 'Delivery entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

CREATE TABLE if not exists "clean_sch".delivery (
    delivery_sk INT AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)', -- Primary key with auto-increment
    delivery_id INT NOT NULL comment 'Primary Key (Source System)',
    order_id_fk NUMBER NOT NULL comment 'Order FK (Source System)',                        -- Foreign key reference, converted to numeric type
    delivery_agent_id_fk NUMBER NOT NULL comment 'Delivery Agent FK (Source System)',               -- Foreign key reference, converted to numeric type
    delivery_status STRING,                 -- Delivery status, stored as a string
    estimated_time STRING,                  -- Estimated time, stored as a string
    customer_address_id_fk NUMBER NOT NULL  comment 'Customer Address FK (Source System)',                      -- Foreign key reference, converted to numeric type
    delivery_date TIMESTAMP,                -- Delivery date, converted to timestamp
    created_date TIMESTAMP,                 -- Created date, converted to timestamp
    modified_date TIMESTAMP,                -- Modified date, converted to timestamp

    -- Audit columns with appropriate data types
    _stg_file_name STRING,                  -- Source file name
    _stg_file_load_ts TIMESTAMP,            -- Source file load timestamp
    _stg_file_md5 STRING,                   -- MD5 checksum of the source file
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Metadata timestamp
)
comment = 'Delivery entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create table if not exists "clean_sch".orders (
    order_sk number autoincrement primary key comment 'surrogate key (edw)',                -- auto-incremented primary key
    order_id bigint unique comment 'primary key (source system)',                      -- primary key inferred as bigint
    customer_id_fk bigint comment 'customer fk(source system)',                   -- foreign key inferred as bigint
    restaurant_id_fk bigint comment 'restaurant fk(source system)',                 -- foreign key inferred as bigint
    order_date timestamp,                 -- order date inferred as timestamp
    total_amount decimal(10, 2),          -- total amount inferred as decimal with two decimal places
    status string,                        -- status as string
    payment_method string,                -- payment method as string
    created_dt timestamp_tz,              -- record creation date
    modified_dt timestamp_tz,             -- last modified date, allows null if not modified

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp for audit
    _stg_file_md5 string,                                        -- md5 hash for file content for audit
    _copy_data_ts timestamp_ntz default current_timestamp        -- timestamp when data is copied, defaults to current timestamp
)
comment = 'Order entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. this table does not support scd2';

CREATE TABLE if not exists "clean_sch".order_item (
    order_item_sk NUMBER AUTOINCREMENT primary key comment 'Surrogate Key (EDW)',    -- Auto-incremented unique identifier for each order item
    order_item_id NUMBER  NOT NULL UNIQUE comment 'Primary Key (Source System)',
    order_id_fk NUMBER  NOT NULL comment 'Order FK(Source System)',                  -- Foreign key reference for Order ID
    menu_id_fk NUMBER  NOT NULL comment 'Menu FK(Source System)',                   -- Foreign key reference for Menu ID
    quantity NUMBER(10, 2),                 -- Quantity as a decimal number
    price NUMBER(10, 2),                    -- Price as a decimal number
    subtotal NUMBER(10, 2),                 -- Subtotal as a decimal number
    created_dt TIMESTAMP,                 -- Created date of the order item
    modified_dt TIMESTAMP,                -- Modified date of the order item

    -- Audit columns
    _stg_file_name VARCHAR(255),            -- File name of the staging file
    _stg_file_load_ts TIMESTAMP,            -- Timestamp when the file was loaded
    _stg_file_md5 VARCHAR(255),             -- MD5 hash of the file for integrity check
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Timestamp when data is copied into the clean layer
)
comment = 'Order item entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';






