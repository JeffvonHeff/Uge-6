# Bike Shop Database Framework
This document describes the MySQL schema defined in `schema.sql` for loading the
Excel workbook `AllcsvData.xlsx` (and its CSV exports). The schema normalises
the workbook into relational tables, enforcing the relationships outlined in
`RelationsInDataCSV.txt`.
## Entity Relationship Overview
```
customers (1) ──< orders >── (1) stores
                       │
                       └── staffs
orders (1) ──< order_items >── (1) products
products (1) ── categories
products (1) ── brands
stores (1) ──< stocks >── (1) products
staffs (self) ── managers
```
Key relationships:
- `orders.customer_id` → `customers.customer_id`
- `orders.store_id` → `stores.store_id`
- `orders.staff_id` → `staffs.staff_id`
- `order_items.product_id` → `products.product_id`
- `products.brand_id` → `brands.brand_id`
- `products.category_id` → `categories.category_id`
- `stocks` links each `store_id` with its on-hand `product_id` quantity.
- `staffs.manager_id` self-references `staffs.staff_id`.
## Loading Strategy
1. **Lookup tables** – load `brands.csv`, `categories.csv`, and `stores.csv`.
   - Use the provided `store_name` values to populate `stores.store_name`. The
     `store_id` surrogate key is generated automatically.
2. **Core reference data** – load `customers.csv`, `products.csv`, and
   `staffs.csv`.
   - When loading staff members, translate the `store_name` column to the
     corresponding `store_id` from `stores` and copy the `street` value into
     `staffs.street`.
   - Populate the optional `manager_id` values after all staff rows have been
     inserted, using the mapping between manager names and generated IDs.
3. **Inventory and transactional data** – load `stocks.csv`, `orders.csv`, and
   `order_items.csv`.
   - Convert `orders.store` into `store_id` by joining on `stores.store_name`.
   - Replace `orders.staff_name` with the matching `staff_id`.
   - Parse all date columns (`order_date`, `required_date`, `shipped_date`) to
     `DATE` values using the format `DD/MM/YYYY`.
   - Load `order_items` after `orders` and `products` to satisfy foreign keys.
4. **Validation** – enable foreign key checks (`SET FOREIGN_KEY_CHECKS = 1;`)
   after bulk loading to verify referential integrity.
## Useful Queries
```sql
-- Total revenue per store
SELECT s.store_name,
       SUM(oi.quantity * oi.list_price * (1 - oi.discount)) AS revenue
FROM orders o
JOIN stores s ON s.store_id = o.store_id
JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY s.store_name
ORDER BY revenue DESC;
-- Inventory position per store
SELECT s.store_name, p.product_name, st.quantity
FROM stocks st
JOIN stores s ON s.store_id = st.store_id
JOIN products p ON p.product_id = st.product_id
ORDER BY s.store_name, p.product_name;
```