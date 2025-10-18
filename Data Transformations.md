
Normalize phone numbers - Goal: convert messy phone into canonical E.164-like +91XXXXXXXXXX where possible.

Clean product category (product_category_raw) -> canonical product_category


##############################################################################################################################
#######################################IGNORE BELOW############################################################################

Derive channel from sessions / utm / user_agent - Task: orders have channel and utm_source but sometimes missing. Build a derived order_channel_final using priority:

    If channel present, use it.

    Else if utm_source present, map utm->channel (e.g., google/facebook -> web).

    Else infer from device_type and user_agent (mobile -> mobile_app if app UA, else web).

Parse user_agent to get browser name & version (basic):

  'WHEN user_agent LIKE '%Chrome/%' AND user_agent NOT LIKE '%Mobile%' THEN 'Chrome'
  
  WHEN user_agent LIKE '%Firefox/%' THEN 'Firefox'
  
  WHEN user_agent LIKE '%Safari/%' AND user_agent NOT LIKE '%Chrome/%' THEN 'Safari'
  
  WHEN user_agent LIKE '%Mobile%' OR user_agent LIKE '%iPhone%' THEN CONCAT('Mobile-', CASE WHEN user_agent LIKE '%Chrome/%' THEN 'Chrome' ELSE 'Safari' END)
  
  ELSE 'Other' '


  
