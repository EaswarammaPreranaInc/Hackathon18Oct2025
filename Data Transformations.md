
Normalize phone numbers - Goal: convert messy phone into canonical E.164-like +91XXXXXXXXXX where possible.

Clean product category (product_category_raw) -> canonical product_category

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

Give each team one or two of these transformation tasks (mix easy/harder):

Phone normalization + match rate — create phone_norm, compute % matched to 10-digit numbers.

Category mapping — produce canonical product_category and top-5 categories by revenue both before & after mapping.

Channel derivation — create order_channel_final and show revenue by derived channel; compare with raw channel.

Browser parsing — create browser column and show conversion rate (orders / sessions) per browser.

Revenue recognition comparison — compute monthly revenue by order/payment/delivered date and argue which is correct.
