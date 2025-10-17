# Full KPI Catalogue — E-commerce Hackathon

## GMV
**Description:** Sum of order_items.line_total for completed orders
**Grain:** order_item
**Source fields:** order_items.line_total, orders.status
**SQL:**
```sql
SELECT SUM(oi.line_total) AS gmv FROM order_items oi JOIN orders o ON o.order_id=oi.order_id WHERE o.status='COMPLETED';
```

## Net Revenue
**Description:** GMV minus refunds/refund_amounts
**Grain:** order_item/returns
**Source fields:** order_items.line_total, returns.refund_amount (or payments.is_refunded)
**SQL:**
```sql
SELECT SUM(oi.line_total) - COALESCE(SUM(r.refund_amount),0) AS net_revenue FROM order_items oi JOIN orders o ON o.order_id=oi.order_id LEFT JOIN returns r ON r.order_item_id=oi.order_item_id WHERE o.status='COMPLETED';
```

## AOV
**Description:** Average order value (mean order_value for completed orders)
**Grain:** order
**Source fields:** orders.order_value
**SQL:**
```sql
SELECT AVG(order_value) AS aov FROM orders WHERE status='COMPLETED';
```

## Refund Rate ($)
**Description:** Total refund amount / GMV
**Grain:** period
**Source fields:** returns.refund_amount, order_items.line_total
**SQL:**
```sql
SELECT SUM(r.refund_amount)/SUM(oi.line_total) AS refund_rate FROM returns r JOIN order_items oi ON oi.order_item_id=r.order_item_id;
```

## Orders Count
**Description:** Count of completed orders
**Grain:** order
**Source fields:** orders.order_id
**SQL:**
```sql
SELECT COUNT(*) FROM orders WHERE status='COMPLETED';
```

## Gross Margin
**Description:** (Revenue - COGS) / Revenue using product.cost_price
**Grain:** order_item
**Source fields:** order_items.line_total, products.cost_price
**SQL:**
```sql
SELECT (SUM(oi.line_total) - SUM(oi.quantity * p.cost_price))/SUM(oi.line_total) AS gross_margin FROM order_items oi JOIN products p ON p.product_id=oi.product_id;
```

## Average Delivery Time
**Description:** Average days between shipped_date and delivered_date
**Grain:** shipment
**Source fields:** shipments.shipped_date, shipments.delivered_date
**SQL:**
```sql
SELECT AVG(DATEDIFF(delivered_date, shipped_date)) AS avg_delivery_days FROM shipments WHERE delivered_date IS NOT NULL;
```

## On-time Delivery Rate
**Description:** Pct delivered within SLA days
**Grain:** shipment
**Source fields:** shipments
**SQL:**
```sql
Define SLA (e.g., 5 days) and compute fraction
```

## Fulfillment Rate
**Description:** Pct of orders shipped
**Grain:** order
**Source fields:** shipments
**SQL:**
```sql
COUNT(shipped)/COUNT(orders)
```

## Customer Count (active)
**Description:** Distinct customers with >=1 completed order
**Grain:** customer
**Source fields:** orders.customer_id
**SQL:**
```sql
SELECT COUNT(DISTINCT customer_id) FROM orders WHERE status='COMPLETED';
```

## Repeat Purchase Rate
**Description:** Pct customers with >1 completed orders
**Grain:** customer
**Source fields:** orders
**SQL:**
```sql
WITH c AS (SELECT customer_id, COUNT(*) cnt FROM orders WHERE status='COMPLETED' GROUP BY 1) SELECT SUM(CASE WHEN cnt>1 THEN 1 ELSE 0 END)/COUNT(*) FROM c;
```

## LTV (6-month historic)
**Description:** Total revenue from customers in cohort in 6 months / number of cohort customers
**Grain:** cohort
**Source fields:** customers.signup_date, orders
**SQL:**
```sql
Requires cohorting by signup month and summing revenue for next 6 months.
```

## Churn Rate
**Description:** Pct customers active previous period not active this period
**Grain:** cohort
**Source fields:** orders by period
**SQL:**
```sql
Compute retention matrices.
```

## Sessions
**Description:** Count of sessions
**Grain:** session
**Source fields:** sessions.session_id
**SQL:**
```sql
SELECT COUNT(*) FROM sessions WHERE session_start BETWEEN ...;
```

## Conversion Rate (session->order)
**Description:** Sessions with order / total sessions
**Grain:** session
**Source fields:** sessions, order_sessions
**SQL:**
```sql
SELECT COUNT(DISTINCT os.session_id)/COUNT(*) FROM sessions s LEFT JOIN order_sessions os ON s.session_id=os.session_id;
```

## Top Sellers
**Description:** Top products by revenue and units
**Grain:** product
**Source fields:** order_items, products
**SQL:**
```sql
SELECT p.product_id, SUM(oi.line_total) FROM order_items oi JOIN products p ON p.product_id=oi.product_id GROUP BY p.product_id ORDER BY SUM(oi.line_total) DESC;
```

## Category Revenue
**Description:** Revenue by canonical category (after mapping)
**Grain:** category
**Source fields:** order_items, products.cleaned_category
**SQL:**
```sql
Requires mapping product_category_raw -> canonical first.
```

## Refunds by Reason
**Description:** Count and amount of refunds grouped by reason_code (returns)
**Grain:** returns
**Source fields:** returns.reason_code, returns.refund_amount
**SQL:**
```sql
SELECT reason_code, COUNT(*), SUM(refund_amount) FROM returns GROUP BY reason_code;
```
