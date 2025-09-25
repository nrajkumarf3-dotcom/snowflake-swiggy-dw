use warehouse adhoc_wh;

use role sysadmin;

use database "sandbox";

create table if not exists "consumption_sch".restaurant_location_dim (
    restaurant_location_hk NUMBER primary key,                      -- hash key for the dimension
    location_id number(38,0) not null,                  -- business key
    city varchar(100) not null,                         -- city
    state varchar(100) not null,                        -- state
    state_code varchar(2) not null,                     -- state code
    is_union_territory boolean not null default false,   -- union territory flag
    capital_city_flag boolean not null default false,     -- capital city flag
    city_tier varchar(6),                               -- city tier
    zip_code varchar(10) not null,                      -- zip code
    active_flag varchar(10) not null,                   -- active flag (indicating current record)
    eff_start_dt timestamp_tz(9) not null,              -- effective start date for scd2
    eff_end_dt timestamp_tz(9),                         -- effective end date for scd2
    current_flag boolean not null default true         -- indicator of the current record
)
comment = 'Dimension table for restaurant location with scd2 (slowly changing dimension) enabled and hashkey as surrogate key';

create table if not exists "consumption_sch".restaurant_dim (
    restaurant_hk number primary key,                   -- hash key for the restaurant location
    restaurant_id number,                   -- restaurant id without auto-increment
    name string(100),                       -- restaurant name
    cuisine_type string,                    -- type of cuisine offered
    pricing_for_two number(10, 2),          -- pricing for two people
    restaurant_phone string(15) with tag ("common".pii_policy_tag = 'SENSITIVE'),            -- restaurant phone number
    operating_hours string(100),            -- restaurant operating hours
    location_id_fk number,                  -- foreign key reference to location
    active_flag string(10),                 -- indicates if the restaurant is active
    open_status string(10),                 -- indicates if the restaurant is currently open
    locality string(100),                   -- locality of the restaurant
    restaurant_address string,              -- full address of the restaurant
    latitude number(9, 6),                  -- latitude for the restaurant's location
    longitude number(9, 6),                 -- longitude for the restaurant's location
    eff_start_date timestamp_tz,            -- effective start date for the record
    eff_end_date timestamp_tz,              -- effective end date for the record (null if active)
    is_current boolean                     -- indicates whether the record is the current version
)
comment = 'dimensional table for restaurant entity with hash keys and scd enabled.';

create table if not exists "consumption_sch".customer_dim (
    customer_hk number primary key,               -- surrogate key for the customer
    customer_id string not null,                                 -- natural key for the customer
    name string(100) not null,                                   -- customer name
    mobile string(15) with tag ("common".pii_policy_tag = 'PII'),                                           -- mobile number
    email string(100) with tag ("common".pii_policy_tag = 'EMAIL'),                                           -- email
    login_by_using string(50),                                   -- method of login
    gender string(10) with tag ("common".pii_policy_tag = 'PII'),                                           -- gender
    dob date with tag ("common".pii_policy_tag = 'PII'),                                                    -- date of birth
    anniversary date,                                            -- anniversary
    preferences string,                                          -- preferences
    eff_start_date timestamp_tz,                                 -- effective start date
    eff_end_date timestamp_tz,                                   -- effective end date (null if active)
    is_current boolean                                           -- flag to indicate the current record
)
comment = 'customer dimension table with scd type 2 handling for historical tracking.';

create table if not exists "consumption_sch".customer_address_dim (
    customer_address_hk number primary key comment 'customer address hk (edw)',        -- surrogate key (hash key)
    address_id int comment 'primary key (source system)',                                -- original primary key
    customer_id_fk string comment 'customer fk (source system)',                            -- surrogate key from customer dimension (foreign key)
    flat_no string,                                -- flat number
    house_no string,                               -- house number
    floor string,                                  -- floor
    building string,                               -- building name
    landmark string,                               -- landmark
    locality string,                               -- locality
    city string,                                   -- city
    state string,                                  -- state
    pincode string,                                -- pincode
    coordinates string,                            -- geo-coordinates
    primary_flag string,                           -- whether it's the primary address
    address_type string,                           -- type of address (e.g., home, office)

    -- scd2 columns
    eff_start_date timestamp_tz,                                 -- effective start date
    eff_end_date timestamp_tz,                                   -- effective end date (null if active)
    is_current boolean                                           -- flag to indicate the current record
);

create table if not exists "consumption_sch".menu_dim (
    menu_dim_hk number primary key comment 'menu dim hk (edw)',                         -- hash key generated for menu dim table
    menu_id int not null comment 'primary key (source system)',                       -- unique and non-null menu_id
    restaurant_id_fk int not null comment 'restaurant fk (source system)',                          -- identifier for the restaurant
    item_name string,                            -- name of the menu item
    description string,                         -- description of the menu item
    price decimal(10, 2),                       -- price as a numeric value with 2 decimal places
    category string,                            -- food category (e.g., north indian)
    availability boolean,                       -- availability status (true/false)
    item_type string,                           -- dietary classification (e.g., vegan)
    eff_start_date timestamp_ntz,               -- effective start date of the record
    eff_end_date timestamp_ntz,                 -- effective end date of the record
    is_current boolean                         -- flag to indicate if the record is current (true/false)
)
comment = 'this table stores the dimension data for the menu items, tracking historical changes using scd type 2. each menu item has an effective start and end date, with a flag indicating if it is the current record or historical. the hash key (menu_dim_hk) is generated based on menu_id and restaurant_id.';


create table if not exists "consumption_sch".delivery_agent_dim (
    delivery_agent_hk number primary key comment 'delivery agend dim hk (edw)',               -- hash key for unique identification
    delivery_agent_id number not null comment 'primary key (source system)',               -- business key
    name string not null,                   -- delivery agent name
    phone string unique,                    -- phone number, unique
    vehicle_type string,                    -- type of vehicle
    location_id_fk number not null comment 'location fk (source system)',                     -- location id
    status string,                          -- current status of the delivery agent
    gender string,                          -- gender
    rating number(4,2),                     -- rating with one decimal precision
    eff_start_date timestamp default current_timestamp, -- effective start date
    eff_end_date timestamp,                 -- effective end date (null for active record)
    is_current boolean default true
)
comment =  'dim table for delivery agent entity with scd2 support.';


create table if not exists "consumption_sch".date_dim (
    date_dim_hk number primary key comment 'menu dim hk (edw)',   -- surrogate key for date dimension
    calendar_date date unique,                     -- the actual calendar date
    year number,                                   -- year
    quarter number,                                -- quarter (1-4)
    month number,                                  -- month (1-12)
    week number,                                   -- week of the year
    day_of_year number,                            -- day of the year (1-365/366)
    day_of_week number,                            -- day of the week (1-7)
    day_of_the_month number,                       -- day of the month (1-31)
    day_name string                                -- name of the day (e.g., monday)
)
comment = 'date dimension table created using min of order data.';

create table if not exists "consumption_sch".order_item_fact (
    order_item_fact_sk number autoincrement comment 'surrogate key (edw)', -- surrogate key for the fact table
    order_item_id number  comment 'order item fk (source system)',                    -- natural key from the source data
    order_id number  comment 'order fk (source system)',                         -- reference to the order dimension
    customer_dim_key number  comment 'order fk (source system)',                      -- reference to the customer dimension
    customer_address_dim_key number,                      -- reference to the customer dimension
    restaurant_dim_key number,                    -- reference to the restaurant dimension
    restaurant_location_dim_key number,                    -- reference to the restaurant dimension
    menu_dim_key number,                          -- reference to the menu dimension
    delivery_agent_dim_key number,                -- reference to the delivery agent dimension
    order_date_dim_key number,                         -- reference to the date dimension
    quantity number,                          -- measure
    price number(10, 2),                            -- measure
    subtotal number(10, 2),                         -- measure
    delivery_status varchar,                        -- delivery information
    estimated_time varchar                          -- delivery information
)
comment = 'the item order fact table that has item level price, quantity and other details';

