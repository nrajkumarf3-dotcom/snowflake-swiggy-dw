--Raw/Stage Layer Streams

use warehouse adhoc_wh;
use role sysadmin;

use database "sandbox";

create or replace stream "clean_sch".restaurant_location_stm 
on table "clean_sch".restaurant_location
comment = 'this is a standard stream object on the location table to track insert, update, and delete changes';

create or replace stream "clean_sch".restaurant_stm 
on table "clean_sch".restaurant
comment = 'This is a standard stream object on the clean restaurant table to track insert, update, and delete changes';

create or replace stream "clean_sch".customer_stm 
on table "clean_sch".customer
comment = 'This is the stream object on customer entity to track insert, update, and delete changes';

create or replace stream "clean_sch".customer_address_stm
on table "clean_sch".customer_address
comment = 'This is the stream object on customer address entity to track insert, update, and delete changes';

create or replace stream "clean_sch".menu_stm 
on table "clean_sch".menu
comment = 'This is the stream object on menu table table to track insert, update, and delete changes';

create or replace stream "clean_sch".delivery_agent_stm 
on table "clean_sch".delivery_agent
comment = 'This is the stream object on delivery agent table table to track insert, update, and delete changes';

create or replace stream "clean_sch".delivery_stm 
on table "clean_sch".delivery
comment = 'This is the stream object on delivery agent table table to track insert, update, and delete changes';

create or replace stream "clean_sch".orders_stm 
on table "clean_sch".ORDERS
comment = 'This is the stream object on ORDERS table table to track insert, update, and delete changes';

create or replace stream "clean_sch".order_item_stm 
on table "clean_sch".order_item
comment = 'This is the stream object on order_item table table to track insert, update, and delete changes';









