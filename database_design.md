# Ramme for cykelbutiks-database
Dette dokument beskriver MySQL-skemaet defineret i `schema.sql` til indlæsning af
Excel-arbejdsbogen `AllcsvData.xlsx` (og dens CSV-eksporter). Skemaet normaliserer
arbejdsbogen til relationelle tabeller og håndhæver relationerne beskrevet i
`RelationsInDataCSV.txt`.
## Overblik over entity-relations
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
Vigtige relationer:
- `orders.customer_id` → `customers.customer_id`
- `orders.store_id` → `stores.store_id`
- `orders.staff_id` → `staffs.staff_id`
- `order_items.product_id` → `products.product_id`
- `products.brand_id` → `brands.brand_id`
- `products.category_id` → `categories.category_id`
- `stocks` forbinder hvert `store_id` med den tilgængelige mængde `product_id`.
- `staffs.manager_id` refererer til `staffs.staff_id`.
## Indlæsningsstrategi
1. **Opslagstabeller** – indlæs `brands.csv`, `categories.csv` og `stores.csv`.
   - Brug de angivne `store_name`-værdier til at udfylde `stores.store_name`.
     Surrogatnøglen `store_id` genereres automatisk.
2. **Kerne-referencedata** – indlæs `customers.csv`, `products.csv` og
   `staffs.csv`.
   - Oversæt kolonnen `store_name` til det tilsvarende `store_id` fra `stores`,
     når medarbejdere indlæses, og kopier `street`-værdien til `staffs.street`.
   - Udfyld de valgfrie `manager_id`-værdier, efter alle medarbejdere er
     indsat, ved hjælp af mappingen mellem managernavne og genererede id'er.
3. **Lager- og transaktionsdata** – indlæs `stocks.csv`, `orders.csv` og
   `order_items.csv`.
   - Konverter `orders.store` til `store_id` ved at joine på `stores.store_name`.
   - Erstat `orders.staff_name` med det matchende `staff_id`.
   - Parse alle datokolonner (`order_date`, `required_date`, `shipped_date`) til
     `DATE`-værdier i formatet `DD/MM/YYYY`.
   - Indlæs `order_items` efter `orders` og `products` for at overholde
     fremmednøgler.
4. **Validering** – aktiver fremmednøgletjek (`SET FOREIGN_KEY_CHECKS = 1;`)
   efter bulkload for at verificere referentiel integritet.
## Nyttige forespørgsler
```sql
-- Samlet omsætning pr. butik
SELECT s.store_name,
       SUM(oi.quantity * oi.list_price * (1 - oi.discount)) AS revenue
FROM orders o
JOIN stores s ON s.store_id = o.store_id
JOIN order_items oi ON oi.order_id = o.order_id
GROUP BY s.store_name
ORDER BY revenue DESC;
-- Lagerstatus pr. butik
SELECT s.store_name, p.product_name, st.quantity
FROM stocks st
JOIN stores s ON s.store_id = st.store_id
JOIN products p ON p.product_id = st.product_id
ORDER BY s.store_name, p.product_name;
```
