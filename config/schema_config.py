import datetime

data_schema = {
    "event_date": datetime.date,
    "event_timestamp": datetime.datetime,
    "event_name": str,
    "event_previous_timestamp": datetime.datetime,
    "event_bundle_sequence_id": str,
    "event_server_timestamp_offset": int,
    "user_pseudo_id": str,
    "privacy_info": {
        "analytics_storage": str,
        "ads_storage": str,
        "uses_transient_token": str
    },
    "event_params": list,
    "user_first_touch_timestamp": datetime.datetime,
    "user_properties": list,
    "device": {
        "category": str,
        "mobile_brand_name": str,
        "mobile_model_name": str,
        "mobile_marketing_name": str,
        "mobile_os_hardware_model": str,
        "operating_system": str,
        "operating_system_version": str,
        "language": str,
        "is_limited_ad_tracking": str,
        "time_zone_offset_seconds": int,
        "vendor_id": str
    },
    "geo": {
        "continent": str,
        "country": str,
        "region": str,
        "city": str,
        "sub_continent": str,
        "metro": str
    },
    "app_info": {
        "id": str,
        "version": str,
        "firebase_app_id": str,
        "install_source": str
    },
    "user_ltv": {"revenue": float,
                 "currency": str
                 },
    "platform": str,
    "stream_id": str,
    "device_advertising_id": str,
    "traffic_source": {
        "medium": str,
        "source": str,
        "name": str
    },
    "items": {"item_id": str,
              "item_name": str,
              "item_brand": str,
              "item_variant": str,
              "item_category": str,
              "item_category2": str,
              "item_category3": str,
              "item_category4": str,
              "item_category5": str,
              "price_in_usd": str,
              "price": str,
              "quantity": str,
              "item_revenue_in_usd": str,
              "item_revenue": str,
              "item_refund_in_usd": str,
              "item_refund": str,
              "coupon": str,
              "affiliation": str,
              "location_id": str,
              "item_list_id": str,
              "item_list_name": str,
              "item_list_index": str,
              "promotion_id": str,
              "promotion_name": str,
              "creative_name": str,
              "creative_slot": str}
}
