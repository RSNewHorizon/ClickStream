Before going ahead to the final query execution in the clickstream_flattened_data, I did the dry run for unnesting script both CREATE TABLE in first go and APPEND table thereafter into a SAMPLE table named  `clickstream__summary.clickstream_flattend_data_sample`,  and it went well.

So, I am scheduling this same query for now to test for tomorrow with SAMPLE table as target .Once got executed successfully , I can change target to `clickstream__summary.clickstream_flattend_data`

**RESULTS**:

In FIRST RUN:

**STEP 1:** 

**Creating the SAMPLE_TARGET_TABLE in FIRST go with data from CURRENTDAY-3**

TABLE NAME: `clickstream__summary.clickstream_flattend_data_sample`



```sql
#UDF for event Params

CREATE temp FUNCTION paramvaluebykey(k string, params array<struct<KEY string, value struct<string_value string, int_value int64, float_value float64, double_value float64 >>>) AS (
(
       SELECT x.value
       FROM   unnest(params) x
       WHERE  x.KEY=k) );


 
#UDF for user_properties

CREATE temp FUNCTION propertyvaluebykey(k string, properties array<struct<KEY string, value struct<string_value string, int_value int64, float_value float64, double_value float64,set_timestamp_micros int64 >>>) AS (
(
       SELECT x.value
       FROM   unnest(properties) x
       WHERE  x.KEY=k) );

#Create unnested table
 
CREATE TABLE `clickstream__summary.clickstream_flattend_data_sample`
PARTITION BY event_date AS
  (SELECT
          Parse_date('%Y%m%d', event_date) AS event_date ,
          event_timestamp,
          event_name,
          Paramvaluebykey('ga_session_id', event_params).int_value
  AS
  ep_ga_session_id,
          Paramvaluebykey('item_id', event_params).string_value
  AS ep_item_id,
          Paramvaluebykey('campaign_info_source', event_params).string_value
  AS
          ep_campaign_info_source,
          Paramvaluebykey('medium', event_params).string_value
  AS ep_medium,
          Paramvaluebykey('engaged_session_event', event_params).int_value
  AS
          ep_engaged_session_event,
          Paramvaluebykey('firebase_event_origin', event_params).string_value
  AS
          ep_firebase_event_origin,
          Paramvaluebykey('source', event_params).string_value
  AS ep_source,
          Paramvaluebykey('ga_session_number', event_params).int_value
  AS
          ep_ga_session_number,
          Paramvaluebykey('firebase_screen_class', event_params).string_value
  AS
          ep_firebase_screen_class,
          Paramvaluebykey('engagement_time_msec', event_params).int_value
  AS
          ep_engagement_time_msec,
          Paramvaluebykey('firebase_screen_id', event_params).int_value
  AS
          ep_firebase_screen_id,
          Paramvaluebykey('firebase_previous_id', event_params).int_value
  AS
          ep_firebase_previous_id,
          Paramvaluebykey('firebase_previous_screen', event_params).string_value
  AS
          ep_firebase_previous_screen,
          Paramvaluebykey('firebase_previous_class', event_params).string_value
  AS
          ep_firebase_previous_class,
          Paramvaluebykey('app', event_params).string_value
  AS ep_app,
          Paramvaluebykey('Id_Search', event_params).int_value
  AS ep_Id_Search,
          Paramvaluebykey('user', event_params).string_value
  AS ep_user,
          Paramvaluebykey('firebase_screen', event_params).string_value
  AS
          ep_firebase_screen,
          Paramvaluebykey('content_type', event_params).string_value
  AS ep_content_type,
          Paramvaluebykey('content', event_params).string_value
  AS ep_content,
          Paramvaluebykey('firebase_conversion', event_params).int_value
  AS
          ep_firebase_conversion,
          Paramvaluebykey('session_engaged', event_params).int_value
  AS ep_session_engaged,
          Paramvaluebykey('previous_os_version', event_params).string_value
  AS
          ep_previous_os_version,
          Paramvaluebykey('previous_first_open_count', event_params).int_value
  AS
          ep_previous_first_open_count,
          Paramvaluebykey('system_app_update', event_params).int_value
  AS
          ep_system_app_update,
          Paramvaluebykey('update_with_analytics', event_params).int_value
  AS
          update_with_analytics,
          Paramvaluebykey('system_app', event_params).int_value
  AS ep_system_app,
          Paramvaluebykey('previous_app_version', event_params).string_value
  AS
          ep_previous_app_version,
          Paramvaluebykey('callPerformed', event_params).int_value
  AS ep_callPerformed,
          Paramvaluebykey('SmaTrends', event_params).int_value
  AS ep_SmaTrends,
          Paramvaluebykey('adhikari_screen_open', event_params).int_value
  AS
          ep_adhikari_screen_open,
          Paramvaluebykey('mappedAdhikari', event_params).int_value
  AS ep_mappedAdhikari,
          Paramvaluebykey('loginUser', event_params).string_value
  AS ep_loginUser,
          Paramvaluebykey('login', event_params).string_value
  AS ep_login,
          Paramvaluebykey('timestamp', event_params).int_value
  AS ep_timestamp,
          Paramvaluebykey('fatal', event_params).int_value
  AS ep_fatal,
          Paramvaluebykey('ConsumerAPP', event_params).int_value
  AS ep_item_name,
          Paramvaluebykey('interaction', event_params).int_value
  AS ep_interaction,
          Paramvaluebykey('mobile', event_params).string_value
  AS ep_mobile,
          Paramvaluebykey('click', event_params).int_value
  AS ep_click,
          event_previous_timestamp,
          event_value_in_usd,
          event_bundle_sequence_id,
          event_server_timestamp_offset,
          user_id,
          user_pseudo_id,
          privacy_info.analytics_storage
  AS privacy_info_analytics_storage,
          privacy_info.ads_storage
  AS privacy_info_ads_storage,
          privacy_info.uses_transient_token
  AS privacy_info_uses_transient_token,
          Propertyvaluebykey('user_id', user_properties).string_value
  AS up_user_id,
          Propertyvaluebykey('first_open_time', user_properties).int_value
  AS
          up_first_open_time,
          Propertyvaluebykey('ga_session_id', user_properties).int_value
  AS
          up_ga_session_id,
          Propertyvaluebykey('ga_session_number', user_properties).int_value
  AS
          up_ga_session_number,
          user_first_touch_timestamp,
          user_ltv.revenue
  AS user_ltv_revenue,
          user_ltv.currency
  AS user_ltv_currency,
          device.category
  AS device_category,
          device.mobile_brand_name
  AS device_mobile_brand_name,
          device.mobile_model_name
  AS device_mobile_model_name,
          device.mobile_marketing_name
  AS device_mobile_marketing_name,
          device.mobile_os_hardware_model
  AS device_mobile_os_hardware_model,
          device.operating_system
  AS device_operating_system,
          device.operating_system_version
  AS device_operating_system_version,
          device.vendor_id
  AS device_vendor_id,
          device.advertising_id
  AS device_advertising_id,
          device.language
  AS device_language,
          device.is_limited_ad_tracking
  AS device_is_limited_ad_tracking,
          device.time_zone_offset_seconds
  AS device_time_zone_offset_seconds,
          device.browser
  AS device_browser,
          device.browser_version
  AS device_browser_version,
          device.web_info.browser
  AS device_web_info_browser,
          device.web_info.browser_version
  AS device_web_info_browser_version,
          device.web_info.hostname
  AS device_web_info_hostname,
          geo.continent
  AS geo_continent,
          geo.country
  AS geo_country,
          geo.region
  AS geo_region,
          geo.city
  AS geo_city,
          geo.sub_continent
  AS geo_sub_continent,
          geo.metro
  AS geo_metro,
          app_info.id
  AS app_info_id,
          app_info.version
  AS app_info_version,
          app_info.install_store
  AS app_info_install_store,
          app_info.firebase_app_id
  AS app_info_firebase_app_id,
          app_info.install_source
  AS app_info_install_source,
          traffic_source.name
  AS traffic_source_name,
          traffic_source.medium
  AS traffic_source_medium,
          traffic_source.source
  AS traffic_source_source,
          stream_id,
          platform,
          event_dimensions.hostname
  AS event_dimensions_hostname,
          from
          `strokenet-151207.analytics_246870031.events_*`where _TABLE_SUFFIX =FORMAT_DATE("%Y%m%d", (DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)))
) 




```



In SECOND RUN:

**STEP 2: Appending data for Day-2 to the sample table.**

````sql
#UDF for event Params

CREATE temp FUNCTION paramvaluebykey(k string, params array<struct<KEY string, value struct<string_value string, int_value int64, float_value float64, double_value float64 >>>) AS (
(
       SELECT x.value
       FROM   unnest(params) x
       WHERE  x.KEY=k) );


 
#UDF for user_properties

CREATE temp FUNCTION propertyvaluebykey(k string, properties array<struct<KEY string, value struct<string_value string, int_value int64, float_value float64, double_value float64,set_timestamp_micros int64 >>>) AS (
(
       SELECT x.value
       FROM   unnest(properties) x
       WHERE  x.KEY=k) );

#Create unnested table
 
insert into  `clickstream__summary.clickstream_flattend_data_sample`
  (SELECT
          Parse_date('%Y%m%d', event_date) AS event_date ,
          event_timestamp,
          event_name,
          Paramvaluebykey('ga_session_id', event_params).int_value
  AS
  ep_ga_session_id,
          Paramvaluebykey('item_id', event_params).string_value
  AS ep_item_id,
          Paramvaluebykey('campaign_info_source', event_params).string_value
  AS
          ep_campaign_info_source,
          Paramvaluebykey('medium', event_params).string_value
  AS ep_medium,
          Paramvaluebykey('engaged_session_event', event_params).int_value
  AS
          ep_engaged_session_event,
          Paramvaluebykey('firebase_event_origin', event_params).string_value
  AS
          ep_firebase_event_origin,
          Paramvaluebykey('source', event_params).string_value
  AS ep_source,
          Paramvaluebykey('ga_session_number', event_params).int_value
  AS
          ep_ga_session_number,
          Paramvaluebykey('firebase_screen_class', event_params).string_value
  AS
          ep_firebase_screen_class,
          Paramvaluebykey('engagement_time_msec', event_params).int_value
  AS
          ep_engagement_time_msec,
          Paramvaluebykey('firebase_screen_id', event_params).int_value
  AS
          ep_firebase_screen_id,
          Paramvaluebykey('firebase_previous_id', event_params).int_value
  AS
          ep_firebase_previous_id,
          Paramvaluebykey('firebase_previous_screen', event_params).string_value
  AS
          ep_firebase_previous_screen,
          Paramvaluebykey('firebase_previous_class', event_params).string_value
  AS
          ep_firebase_previous_class,
          Paramvaluebykey('app', event_params).string_value
  AS ep_app,
          Paramvaluebykey('Id_Search', event_params).int_value
  AS ep_Id_Search,
          Paramvaluebykey('user', event_params).string_value
  AS ep_user,
          Paramvaluebykey('firebase_screen', event_params).string_value
  AS
          ep_firebase_screen,
          Paramvaluebykey('content_type', event_params).string_value
  AS ep_content_type,
          Paramvaluebykey('content', event_params).string_value
  AS ep_content,
          Paramvaluebykey('firebase_conversion', event_params).int_value
  AS
          ep_firebase_conversion,
          Paramvaluebykey('session_engaged', event_params).int_value
  AS ep_session_engaged,
          Paramvaluebykey('previous_os_version', event_params).string_value
  AS
          ep_previous_os_version,
          Paramvaluebykey('previous_first_open_count', event_params).int_value
  AS
          ep_previous_first_open_count,
          Paramvaluebykey('system_app_update', event_params).int_value
  AS
          ep_system_app_update,
          Paramvaluebykey('update_with_analytics', event_params).int_value
  AS
          update_with_analytics,
          Paramvaluebykey('system_app', event_params).int_value
  AS ep_system_app,
          Paramvaluebykey('previous_app_version', event_params).string_value
  AS
          ep_previous_app_version,
          Paramvaluebykey('callPerformed', event_params).int_value
  AS ep_callPerformed,
          Paramvaluebykey('SmaTrends', event_params).int_value
  AS ep_SmaTrends,
          Paramvaluebykey('adhikari_screen_open', event_params).int_value
  AS
          ep_adhikari_screen_open,
          Paramvaluebykey('mappedAdhikari', event_params).int_value
  AS ep_mappedAdhikari,
          Paramvaluebykey('loginUser', event_params).string_value
  AS ep_loginUser,
          Paramvaluebykey('login', event_params).string_value
  AS ep_login,
          Paramvaluebykey('timestamp', event_params).int_value
  AS ep_timestamp,
          Paramvaluebykey('fatal', event_params).int_value
  AS ep_fatal,
          Paramvaluebykey('ConsumerAPP', event_params).int_value
  AS ep_item_name,
          Paramvaluebykey('interaction', event_params).int_value
  AS ep_interaction,
          Paramvaluebykey('mobile', event_params).string_value
  AS ep_mobile,
          Paramvaluebykey('click', event_params).int_value
  AS ep_click,
          event_previous_timestamp,
          event_value_in_usd,
          event_bundle_sequence_id,
          event_server_timestamp_offset,
          user_id,
          user_pseudo_id,
          privacy_info.analytics_storage
  AS privacy_info_analytics_storage,
          privacy_info.ads_storage
  AS privacy_info_ads_storage,
          privacy_info.uses_transient_token
  AS privacy_info_uses_transient_token,
          Propertyvaluebykey('user_id', user_properties).string_value
  AS up_user_id,
          Propertyvaluebykey('first_open_time', user_properties).int_value
  AS
          up_first_open_time,
          Propertyvaluebykey('ga_session_id', user_properties).int_value
  AS
          up_ga_session_id,
          Propertyvaluebykey('ga_session_number', user_properties).int_value
  AS
          up_ga_session_number,
          user_first_touch_timestamp,
          user_ltv.revenue
  AS user_ltv_revenue,
          user_ltv.currency
  AS user_ltv_currency,
          device.category
  AS device_category,
          device.mobile_brand_name
  AS device_mobile_brand_name,
          device.mobile_model_name
  AS device_mobile_model_name,
          device.mobile_marketing_name
  AS device_mobile_marketing_name,
          device.mobile_os_hardware_model
  AS device_mobile_os_hardware_model,
          device.operating_system
  AS device_operating_system,
          device.operating_system_version
  AS device_operating_system_version,
          device.vendor_id
  AS device_vendor_id,
          device.advertising_id
  AS device_advertising_id,
          device.language
  AS device_language,
          device.is_limited_ad_tracking
  AS device_is_limited_ad_tracking,
          device.time_zone_offset_seconds
  AS device_time_zone_offset_seconds,
          device.browser
  AS device_browser,
          device.browser_version
  AS device_browser_version,
          device.web_info.browser
  AS device_web_info_browser,
          device.web_info.browser_version
  AS device_web_info_browser_version,
          device.web_info.hostname
  AS device_web_info_hostname,
          geo.continent
  AS geo_continent,
          geo.country
  AS geo_country,
          geo.region
  AS geo_region,
          geo.city
  AS geo_city,
          geo.sub_continent
  AS geo_sub_continent,
          geo.metro
  AS geo_metro,
          app_info.id
  AS app_info_id,
          app_info.version
  AS app_info_version,
          app_info.install_store
  AS app_info_install_store,
          app_info.firebase_app_id
  AS app_info_firebase_app_id,
          app_info.install_source
  AS app_info_install_source,
          traffic_source.name
  AS traffic_source_name,
          traffic_source.medium
  AS traffic_source_medium,
          traffic_source.source
  AS traffic_source_source,
          stream_id,
          platform,
          event_dimensions.hostname
  AS event_dimensions_hostname,
          from
          `strokenet-151207.analytics_246870031.events_*`where _TABLE_SUFFIX =FORMAT_DATE("%Y%m%d", (DATE_ADD(CURRENT_DATE(), INTERVAL -2 DAY)))
) 




````

**HUMAN Readable event_datetime**

-select distinct(event_date) from `clickstream__summary.clickstream_flattend_data_sample`

-SELECT TIMESTAMP_MICROS(event_timestamp) from `clickstream__summary.clickstream_flattend_data_sample`  





select distinct(event_date) from `clickstream__summary.clickstream_flattend_data_sample` 

select distinct(event_date) from `clickstream_summary.clickstream_flattend_data`



