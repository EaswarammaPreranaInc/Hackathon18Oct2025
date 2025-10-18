
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


  Code to transform the product category

  # Define a function to clean and standardize category names
def clean_category(value):
    if not isinstance(value, str):
        return 'Unknown'
    
    # Basic normalization
    val = value.strip().lower()
    val = re.sub(r'[^a-z]', '', val)  # remove non-alphabetic characters

    # Mapping logic
    if 'apparel' in val or 'apprel' in val:
        return 'Apparel'
    elif 'beauty' in val or 'care' in val:
        return 'Beauty'
    elif 'book' in val:
        return 'Books'
    elif 'electronic' in val or 'electronix' in val or 'elec' in val:
        return 'Electronics'
    elif 'grocery' in val or 'gr0cery' in val:
        return 'Grocery'
    elif 'home' in val:
        return 'Home'
    elif 'sport' in val:
        return 'Sports'
    elif 'toy' in val:
        return 'Toys'
    else:
        return 'Other'

# Apply the transformation
df['product_category_clean'] = df['product_category_raw'].apply(clean_category)


Phone number transformation 

# --- Function to clean phone numbers ---
    def clean_phone(value):
        if pd.isna(value):
            return np.nan
        digits = re.sub(r'\D', '', str(value))  # keep only digits
        if len(digits) >= 10:
            return digits[-10:]  # last 10 digits
        else:
            return np.nan  # invalid number

# Apply transformation
    df['phone_clean'] = df['phone_raw'].apply(clean_phone)


