alter user PRASKU
 set default_role = 'SYSADMIN';
alter user PRASKU
 set default_warehouse = 'COMPUTE_WH_SA';
alter user PRASKU
 set default_namespace = 'UTIL_DB.PUBLIC';

use role accountadmin;

select util_db.public.grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123 ) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 

USE ROLE SYSADMIN;
CREATE DATABASE AGS_GAME_AUDIENCE;
USE DATABASE AGS_GAME_AUDIENCE;
DROP SCHEMA PUBLIC;
CREATE SCHEMA RAW;
SHOW DATABASES LIKE 'AGS_GAME_AUDIENCE';
SHOW SCHEMAS IN DATABASE AGS_GAME_AUDIENCE;
-- Check ownership of schemas
SHOW GRANTS ON SCHEMA AGS_GAME_AUDIENCE.RAW;

-- Transfer ownership if needed
GRANT OWNERSHIP ON SCHEMA AGS_GAME_AUDIENCE.RAW TO ROLE SYSADMIN;
CREATE OR REPLACE TABLE AGS_GAME_AUDIENCE.RAW.GAME_LOGS (
    RAW_LOG VARIANT
);
CREATE OR REPLACE STAGE AGS_GAME_AUDIENCE.RAW.external_stage
URL = 's3://my-bucket-name'
CREDENTIALS = (AWS_KEY_ID = '<your_aws_key>' AWS_SECRET_KEY = '<your_aws_secret_key>');
LIST @AGS_GAME_AUDIENCE.RAW.S3EXTERNALBUCKET;

select $1 from 
from DNGW_Sample_from_Agnies_Game.json


CREATE OR REPLACE FILE FORMAT AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;
SHOW FILE FORMATS LIKE 'FF_JSON_LOGS' IN SCHEMA AGS_GAME_AUDIENCE.RAW;

SELECT $1 
FROM
    @AGS_GAME_AUDIENCE.RAW.uni_kishore/kickoff/DNGW_Sample_from_Agnies_Game.json
    (FILE_FORMAT => 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')

TRUNCATE TABLE AGS_GAME_AUDIENCE.RAW.GAME_LOGS;
    
copy into AGS_GAME_AUDIENCE.RAW.GAME_LOGS
from @AGS_GAME_AUDIENCE.RAW.uni_kishore/kickoff/DNGW_Sample_from_Agnies_Game.json
file_format = (format_name=FF_JSON_LOGS)
FORCE = TRUE

select
RAW_LOG:agent::text as AGENT,
RAW_LOG:user_event::text as USER_EVENT,
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;


SELECT
    RAW_LOG:agent::STRING AS AGENT,                             -- Extract and cast agent to STRING
    RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ AS EVENT_TIME,      -- Extract and cast datetime_is08601 to TIMESTAMP_NTZ
    RAW_LOG:user_event::STRING AS USER_EVENT,                   -- Extract and cast user_event to STRING
    RAW_LOG:user_login::STRING AS USER_LOGIN,                   -- Extract and cast user_login to STRING
    RAW_LOG:ip_address::STRING AS ip_address,
    RAW_LOG                                                    -- Include the original JSON data
FROM AGS_GAME_AUDIENCE.RAW.GAME_LOGS

CREATE OR REPLACE VIEW AGS_GAME_AUDIENCE.RAW.LOGS AS
SELECT
    -- RAW_LOG:agent::STRING AS AGENT,   
    RAW_LOG:ip_address::STRING AS ip_address,-- Extract and cast agent to STRING
    RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ AS datetime_iso8601,      -- Extract and cast datetime_is08601 to TIMESTAMP_NTZ
    RAW_LOG:user_event::STRING AS USER_EVENT,                   -- Extract and cast user_event to STRING
    RAW_LOG:user_login::STRING AS USER_LOGIN,                   -- Extract and cast user_login to STRING
    
    RAW_LOG                                                    -- Include the original JSON data
FROM AGS_GAME_AUDIENCE.RAW.GAME_LOGS
where ip_address is not null;

select * from AGS_GAME_AUDIENCE.RAW.LOGS

-- DO NOT EDIT THIS CODE
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DNGW01' as step
  ,(
      select count(*)  
      from ags_game_audience.raw.logs
      where is_timestamp_ntz(to_variant(datetime_iso8601))= TRUE 
   ) as actual
, 250 as expected
, 'Project DB and Log File Set Up Correctly' as description
);

select current_timestamp;

--what time zone is your account(and/or session) currently set to? Is it -0700?
select current_timestamp();

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';

create or replace TABLE AGS_GAME_AUDIENCE.RAW.GAME_LOGS (
	RAW_LOG VARIANT
);

ALTER TABLE AGS_GAME_AUDIENCE.RAW.GAME_LOGS
ADD COLUMN IP_ADDRESS VARCHAR;

SELECT * FROM AGS_GAME_AUDIENCE.RAW.GAME_LOGS

delete from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
where ip_address is not null

LIST @AGS_GAME_AUDIENCE.RAW.S3EXTERNALBUCKET/updated_feed;

SELECT $1
-- $1:ip_address::text, $1:
-- RAW_LOG:ip_address::text
FROM
    @AGS_GAME_AUDIENCE.RAW.uni_kishore/updated_feed/DNGW_updated_feed_0_0_0.json 
    (FILE_FORMAT => 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')


SELECT 
    $1:ip_address::STRING AS IP_ADDRESS  -- Extract the ip_address field
  FROM @AGS_GAME_AUDIENCE.RAW.uni_kishore/updated_feed/
(FILE_FORMAT => 'AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS')

    
COPY INTO AGS_GAME_AUDIENCE.RAW.GAME_LOGS 
FROM 
    @AGS_GAME_AUDIENCE.RAW.uni_kishore/updated_feed/DNGW_updated_feed_0_0_0.json 
    file_format = (format_name=FF_JSON_LOGS)
    ON_ERROR = 'CONTINUE'
    FORCE = TRUE;  -- Optional: handle errors without stopping the load - TO FORCE susquent loads

select * from AGS_GAME_AUDIENCE.RAW.LOGS where agent is null
--looking for empty AGENT column
select * 
from ags_game_audience.raw.LOGS
where agent is null;

--looking for non-empty IP_ADDRESS column
select 
RAW_LOG:ip_address::text as IP_ADDRESS
,*
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;

select * from logs
WHERE USER_LOGIN ilike '%kishore%';

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
   'DNGW02' as step
   ,( select sum(tally) from(
        select (count(*) * -1) as tally
        from ags_game_audience.raw.logs 
        union all
        select count(*) as tally
        from ags_game_audience.raw.game_logs)     
     ) as actual
   ,250 as expected
   ,'View is filtered' as description
); 

select parse_ip('107.217.231.17','inet'):host;

create schema ENHANCED;

--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;

--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
, CONVERT_TIMEZONE('UTC',timezone, logs.datetime_iso8601) as game_event_ltz
, DAYNAME(CONVERT_TIMEZONE('UTC',timezone, logs.datetime_iso8601)) AS DOW_NAME
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

-- Your role should be SYSADMIN
-- Your database menu should be set to AGS_GAME_AUDIENCE
-- The schema should be set to RAW

--a Look Up table to convert from hour number to "time of day name"
create or replace table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

--insert statement to add all 24 rows to the table
insert into time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

--Check your table to see if you loaded it properly
select tod_name, listagg(hour,',') 
from time_of_day_lu
group by tod_name;


SELECT 
    logs.ip_address,
    logs.user_login AS GAMER_NAME,
    logs.user_event AS GAME_EVENT_NAME,
    logs.datetime_iso8601 AS GAME_EVENT_UTC,
    loc.city,
    loc.region,
    loc.country,
    loc.timezone AS GAMER_LTZ_NAME,
    CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601)) AS DOW_NAME,
    tod_lu.tod_name AS TOD_NAME
    -- ,tod_lu.hour as is_this_how_i_get_an_hour -- If you need the hour, uncomment this line
FROM 
    AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN 
    IPINFO_GEOLOC.demo.location loc 
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN loc.start_ip_int AND loc.end_ip_int
JOIN 
    time_of_day_lu tod_lu 
    ON EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601)) = tod_lu.hour;


--Wrap any Select in a CTAS statement
create table ags_game_audience.enhanced.logs_enhanced as
SELECT 
    logs.ip_address,
    logs.user_login AS GAMER_NAME,
    logs.user_event AS GAME_EVENT_NAME,
    logs.datetime_iso8601 AS GAME_EVENT_UTC,
    loc.city,
    loc.region,
    loc.country,
    loc.timezone AS GAMER_LTZ_NAME,
    CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601) AS game_event_ltz,
    DAYNAME(CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601)) AS DOW_NAME,
    tod_lu.tod_name AS TOD_NAME
    -- ,tod_lu.hour as is_this_how_i_get_an_hour -- If you need the hour, uncomment this line
FROM 
    AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN 
    IPINFO_GEOLOC.demo.location loc 
    ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) BETWEEN loc.start_ip_int AND loc.end_ip_int
JOIN 
    time_of_day_lu tod_lu 
    ON EXTRACT(HOUR FROM CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601)) = tod_lu.hour;

select * from ags_game_audience.enhanced.logs_enhanced;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT
   'DNGW03' as step
   ,( select count(*) 
      from ags_game_audience.enhanced.logs_enhanced
      where dow_name = 'Sat'
      and tod_name = 'Early evening'   
      and gamer_name like '%prajina'
     ) as actual
   ,2 as expected
   ,'Playing the game on a Saturday evening' as description
); 


use role accountadmin;
--You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task!!
grant execute task on account to role SYSADMIN;

use role sysadmin; 

--Now you should be able to run the task, even if your role is set to SYSADMIN
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;
--the SHOW command might come in handy to look at the task 
show tasks in account;
--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;
--Run the task a few times to see changes in the RUN HISTORY
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--make a note of how many rows you have in the table
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED

--check to see how many rows were added (if any!)
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--first we dump all the rows out of the table
truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

--then we put them all back in
INSERT INTO ags_game_audience.enhanced.LOGS_ENHANCED (
SELECT logs.ip_address 
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, DAYNAME(game_event_ltz) as DOW_NAME
, TOD_NAME
from ags_game_audience.raw.LOGS logs
JOIN ipinfo_geoloc.demo.location loc 
ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
ON HOUR(game_event_ltz) = tod.hour);

--Hey! We should do this every 5 minutes from now until the next millennium - Y3K!!!
--Alexa, play Yeah by Usher!

--clone the table to save this version as a backup
--since it holds the records from the UPDATED FEED file, we'll name it _UF
create table ags_game_audience.enhanced.LOGS_ENHANCED_UF 
clone ags_game_audience.enhanced.LOGS_ENHANCED;


MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING AGS_GAME_AUDIENCE.RAW.LOGS r ON r.user_login = e.GAMER_NAME
AND r.datetime_iso8601 = e.GAME_EVENT_UTC
AND r.user_event = e.GAME_EVENT_NAME
WHEN MATCHED THEN
UPDATE SET IP_ADDRESS = 'Hey, I updated matching rows!';

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where IP_ADDRESS = 'Hey, I updated matching rows!';

truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address 
, logs.user_login as GAMER_NAME
, logs.user_event as GAME_EVENT_NAME
, logs.datetime_iso8601 as GAME_EVENT_UTC
, city
, region
, country
, timezone as GAMER_LTZ_NAME
, CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
, DAYNAME(game_event_ltz) as DOW_NAME
, TOD_NAME
from ags_game_audience.raw.LOGS logs
JOIN ipinfo_geoloc.demo.location loc 
ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
ON HOUR(game_event_ltz) = tod.hour) r --we'll put our fancy select here
ON r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.game_event_utc
and r.GAME_EVENT_NAME = e.game_event_name
WHEN NOT MATCHED THEN
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns
values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME); --list of columns (but we can mark as coming from the r select)

EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Testing cycle for MERGE. Use these commands to make sure the Merge works as expected

--Write down the number of records in your table 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the Merge a few times. No new rows should be added at this time 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if your row count changed 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


--Insert a test record into your Raw Table 
--You can change the user_event field each time to create "new" records 
--editing the ip_address or datetime_iso8601 can complicate things more than they need to 
--editing the user_login will make it harder to remove the fake records after you finish testing 
INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');

select * from ags_game_audience.raw.logs;

--After inserting a new row, run the Merge again 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--When you are confident your merge is working, you can delete the raw records 
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW04' as step
 ,( select count(*)/iff (count(*) = 0, 1, count(*))
  from table(ags_game_audience.information_schema.task_history
              (task_name=>'LOAD_LOGS_ENHANCED'))) as actual
 ,1 as expected
 ,'Task exists and has been run at least once' as description 
 ); 

 Truncate table AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS 
 
COPY INTO AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS 
FROM 
    @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE/
    file_format = (format_name=FF_JSON_LOGS)
    ON_ERROR = 'CONTINUE'
    FORCE = FALSE;  -- Optional: handle errors without stopping the load - TO FORCE susquent loads

SELECT * FROM AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS 

CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    WAREHOUSE = COMPUTE_WH_SA  -- Ensure the warehouse name is correct
    SCHEDULE = '10 MINUTE'
    COMMENT = 'Task to load new files from stage into RAW.GAME_LOGS'
AS
    COPY INTO AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS 
    FROM 
        @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE/
        file_format = (format_name=FF_JSON_LOGS)
        ON_ERROR = 'CONTINUE'
        FORCE = FALSE;  -- Optional: handle errors without stopping the load - TO FORCE susquent loads
ALTER TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES RESUME;

EXECUTE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

SELECT * FROM AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS

SELECT * FROM AGS_GAME_AUDIENCE.RAW.PL_LOGS -- THIS IS A VIEW FROM SOURCE

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

TRUNCATE TABLE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	warehouse=COMPUTE_WH_SA
	schedule='5 minutes'
	as 
    MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
    USING (SELECT logs.ip_address 
    , logs.user_login as GAMER_NAME
    , logs.user_event as GAME_EVENT_NAME
    , logs.datetime_iso8601 as GAME_EVENT_UTC
    , city
    , region
    , country
    , timezone as GAMER_LTZ_NAME
    , CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
    , DAYNAME(game_event_ltz) as DOW_NAME
    , TOD_NAME
    from ags_game_audience.raw.PL_LOGS logs
    JOIN ipinfo_geoloc.demo.location loc 
    ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
    ON HOUR(game_event_ltz) = tod.hour) r --we'll put our fancy select here
    ON r.GAMER_NAME = e.GAMER_NAME
    and r.GAME_EVENT_UTC = e.game_event_utc
    and r.GAME_EVENT_NAME = e.game_event_name
    WHEN NOT MATCHED THEN
    insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns
    values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME);

--Turning on a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

--Turning OFF a task is done with a SUSPEND command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw view (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;

--switch back to sysadmin
use role sysadmin;

USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW05' as step
 ,(
   select max(tally) from (
       select CASE WHEN SCHEDULED_FROM = 'SCHEDULE' 
                         and STATE= 'SUCCEEDED' 
              THEN 1 ELSE 0 END as tally 
   from table(ags_game_audience.information_schema.task_history (task_name=>'GET_NEW_FILES')))
  ) as actual
 ,1 as expected
 ,'Task succeeds from schedule' as description
 ); 


  SELECT 
    METADATA$FILENAME as log_file_name --new metadata column
  , METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
  , current_timestamp(0) as load_ltz --new local time of load
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (file_format => 'ff_json_logs');


CREATE OR REPLACE TABLE AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS AS -- WE WILL CORRECT VARCHAR LENGTH AFTER IT IS FORMED ( CHECK THIS STEP AFTER INTIAL CREATION - VAR CHAR SIZE)
SELECT 
    METADATA$FILENAME as log_file_name,              -- Log file name (metadata)
    METADATA$FILE_ROW_NUMBER as log_file_row_id,     -- Log file row number (metadata)
    CURRENT_TIMESTAMP(0) as load_ltz,                -- Current load timestamp (local time zone)
    get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601, -- Event datetime in UTC (parsed from JSON)
    get($1,'user_event')::text as USER_EVENT,        -- Event type
    get($1,'user_login')::text as USER_LOGIN,        -- User login info
    get($1,'ip_address')::text as IP_ADDRESS         -- User IP address
FROM 
    @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
    (file_format => 'ff_json_logs');

SELECT * FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS

--truncate the table rows that were input during the CTAS, if that's what you did
truncate table ED_PIPELINE_LOGS;

--reload the table using your COPY INTO
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

CREATE OR REPLACE PIPE PIPE_GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

CREATE OR REPLACE TABLE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_BACKUP CLONE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

TRUNCATE TABLE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
    USING (SELECT logs.ip_address 
    , logs.user_login as GAMER_NAME
    , logs.user_event as GAME_EVENT_NAME
    , logs.datetime_iso8601 as GAME_EVENT_UTC
    , city
    , region
    , country
    , timezone as GAMER_LTZ_NAME
    , CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz
    , DAYNAME(game_event_ltz) as DOW_NAME
    , TOD_NAME
    from ags_game_audience.raw.ED_PIPELINE_LOGS logs
    JOIN ipinfo_geoloc.demo.location loc 
    ON ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND ipinfo_geoloc.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    JOIN ags_game_audience.raw.TIME_OF_DAY_LU tod
    ON HOUR(game_event_ltz) = tod.hour) r --we'll put our fancy select here
    ON r.GAMER_NAME = e.GAMER_NAME
    and r.GAME_EVENT_UTC = e.game_event_utc
    and r.GAME_EVENT_NAME = e.game_event_name
    WHEN NOT MATCHED THEN
    insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME) --list of columns
    values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME);

SELECT * FROM AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' ));


--create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--look at the stream you created
show streams;

--check to see if any changes are pending (expect FALSE the first time you run it)
--after the Snowpipe loads a new file, expect to see TRUE
select system$stream_has_data('ed_cdc_stream');

ALTER PIPE AGS_GAME_AUDIENCE.RAW.PIPE_GET_NEW_FILES REFRESH;

--query the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

--if your stream remains empty for more than 10 minutes, make sure your PIPE is running
select SYSTEM$PIPE_STATUS('PIPE_GET_NEW_FILES');

--if you need to pause or unpause your pipe
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = true;
--alter pipe PIPE_GET_NEW_FILES set pipe_execution_paused = false;
--make a note of how many rows are in the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

 
--process the stream by using the rows in a merge 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
 
--Did all the rows from the stream disappear? 
select * 
from ags_game_audience.raw.ed_cdc_stream; 

--Create a new task that uses the MERGE you just tested
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);

--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;

SELECT * FROM AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED


select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW06' as step
 ,(
   select CASE WHEN pipe_status:executionState::text = 'RUNNING' THEN 1 ELSE 0 END 
   from(
   select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.PIPE_GET_NEW_FILES' )) as pipe_status)
  ) as actual
 ,1 as expected
 ,'Pipe exists and is RUNNING' as description
 ); 

alter pipe AGS_GAME_AUDIENCE.RAW.PIPE_GET_NEW_FILES set pipe_execution_paused = true;

USE ROLE SYSADMIN;

CREATE SCHEMA AGS_GAME_AUDIENCE.CURATED;

-- Optional: Verify the owner of the schema
SHOW SCHEMAS LIKE 'CURATED' IN DATABASE AGS_GAME_AUDIENCE;

--the ListAgg function can put both login and logout into a single column in a single row
-- if we don't have a logout, just one timestamp will appear
select GAMER_NAME
      , listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;


 -- Windowed Data for Calculating Time in Game Per Player

select GAMER_NAME
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;


--We added a case statement to bucket the session lengths
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length < 20 then '10 to 19 mins'
            when game_session_length < 30 then '20 to 29 mins'
            when game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_UF)
where logout is not null;




select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW07' as step
 ,( select count(*)/count(*) from snowflake.account_usage.query_history
    where query_text like '%case when game_session_length < 10%'
  ) as actual
 ,1 as expected
 ,'Curated Data Lesson completed' as description
 ); 
