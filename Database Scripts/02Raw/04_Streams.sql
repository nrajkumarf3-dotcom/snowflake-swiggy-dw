--Raw/Stage Layer Streams

use warehouse adhoc_wh;
use role sysadmin;

use database "sandbox";

create or replace stream "stage_sch".location_stm 
on table "stage_sch".location
append_only = true
comment = 'this is the append-only stream object on location table that gets delta data based on changes';

create or replace stream "stage_sch".restaurant_stm 
on table "stage_sch".restaurant
append_only = true
comment = 'This is the append-only stream object on restaurant table that only gets delta data';

create or replace stream "stage_sch".customer_stm 
on table "stage_sch".customer
append_only = true
comment = 'This is the append-only stream object on customer table that only gets delta data';

create or replace stream "stage_sch".customeraddress_stm 
on table "stage_sch".customeraddress
append_only = true
comment = 'This is the append-only stream object on customer address table that only gets delta data';

create or replace stream "stage_sch".menu_stm 
on table "stage_sch".menu
append_only = true
comment = 'This is the append-only stream object on menu entity that only gets delta data';

create or replace stream "stage_sch".deliveryagent_stm 
on table "stage_sch".deliveryagent
append_only = true
comment = 'This is the append-only stream object on delivery agent table that only gets delta data';

create or replace stream "stage_sch".delivery_stm 
on table "stage_sch".delivery
append_only = true
comment = 'this is the append-only stream object on delivery table that only gets delta data';

create or replace stream "stage_sch".orders_stm 
on table "stage_sch".orders
append_only = true
comment = 'This is the append-only stream object on orders entity that only gets delta data';

create or replace stream "stage_sch".orderitem_stm 
on table "stage_sch".orderitem
append_only = true
comment = 'This is the append-only stream object on order item table that only gets delta data';