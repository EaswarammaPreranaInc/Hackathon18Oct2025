Quick guidance: fact tables & grains teams should build

fact_order_item — grain: order_item. Columns: order_item_id (PK), order_id, customer_id, product_id, order_date, order_placed_at, unit_price, quantity, line_total, payment_method, shipment_status, carrier, channel_final, session_id (if available). This is the main fact for product-level revenue analyses.

fact_order — grain: order. Columns: order_id (PK), customer_id, order_date, order_placed_at, order_value, num_items, payment_status, applied_promo_id, order_channel_final. Use for order-level metrics (AOV, orders count).

fact_payment — grain: payment (one per order but may extend). Columns: payment_id, order_id, amount, payment_date, is_refunded.

fact_shipment — grain: shipment. Columns: shipment_id, order_id, shipped_date, delivered_date, carrier, delivery_time_days, shipment_status.

dim_product — SHOULD be designed as SCD if you have price history (not required here). At minimum include: product_id, product_name, product_category_raw, canonical_category (after cleaning), list_price, cost_price.

dim_customer — include customer_id, signup_date, city, state, guest_flag, external_id, phone_norm.

dim_date — full calendar grain; teams should use this for any time aggregation.
