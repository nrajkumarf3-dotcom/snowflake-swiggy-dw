--Raw/Stage Layer Streams

use warehouse adhoc_wh;
use role sysadmin;

use database "sandbox";

use schema "stage_sch";

CREATE OR REPLACE PIPE "stage_sch".location_pipe AS
COPY INTO "stage_sch".location
FROM @stage_csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*cdc/location/.*[.]csv';

CREATE OR REPLACE PIPE "stage_sch".restaurant_pipe AS
COPY INTO "stage_sch".restaurant
FROM @stage_csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*cdc/restaurant/.*[.]csv';

CREATE OR REPLACE PIPE "stage_sch".customer_pipe AS
COPY INTO "stage_sch".customer
FROM  @stage_csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*cdc/customer/.*[.]csv';

CREATE OR REPLACE PIPE "stage_sch".customeraddress_pipe AS
COPY INTO "stage_sch".customeraddress
FROM @stage_csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*cdc/customeraddress/.*[.]csv';

CREATE OR REPLACE PIPE "stage_sch".deliveryagent_pipe AS
COPY INTO "stage_sch".deliveryagent
FROM  @stage_csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*cdc/delivery-agent/.*[.]csv';

CREATE OR REPLACE PIPE "stage_sch".menu_pipe AS
COPY INTO "stage_sch".menu
FROM @stage_csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*cdc/menu/.*[.]csv';

CREATE OR REPLACE PIPE "stage_sch".delivery_pipe AS
COPY INTO "stage_sch".delivery
FROM @stage_csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*cdc/delivery/.*[.]csv';

CREATE OR REPLACE PIPE "stage_sch".orders_pipe AS
COPY INTO "stage_sch".orders
FROM @stage_csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*cdc/order/.*[.]csv';

CREATE OR REPLACE PIPE "stage_sch".orderitem_pipe AS
COPY INTO "stage_sch".orderitem
FROM @stage_csv
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
PATTERN = '.*cdc/order-item/.*[.]csv';