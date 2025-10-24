-- Schema for retail bike store data extracted from AllcsvData.xlsx
-- This script creates the database objects and relationships required to
-- support the supplied CSV sheets.
DROP DATABASE IF EXISTS bike_shop;
CREATE DATABASE bike_shop CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE bike_shop;
-- Lookup tables -----------------------------------------------------------
CREATE TABLE brands (
    brand_id     INT PRIMARY KEY,
    brand_name   VARCHAR(255) NOT NULL,
    UNIQUE KEY uk_brands_name (brand_name)
) ENGINE=InnoDB;
CREATE TABLE categories (
    category_id    INT PRIMARY KEY,
    category_name  VARCHAR(255) NOT NULL,
    UNIQUE KEY uk_categories_name (category_name)
) ENGINE=InnoDB;
CREATE TABLE stores (
    store_id    INT AUTO_INCREMENT PRIMARY KEY,
    store_name  VARCHAR(255) NOT NULL,
    phone       VARCHAR(25),
    email       VARCHAR(255),
    street      VARCHAR(255) NOT NULL,
    city        VARCHAR(100) NOT NULL,
    state       CHAR(2) NOT NULL,
    zip_code    VARCHAR(10) NOT NULL,
    UNIQUE KEY uk_stores_name (store_name),
    INDEX idx_stores_state_city (state, city)
) ENGINE=InnoDB;
-- Core entities -----------------------------------------------------------
CREATE TABLE customers (
    customer_id  INT PRIMARY KEY,
    first_name   VARCHAR(100) NOT NULL,
    last_name    VARCHAR(100) NOT NULL,
    email        VARCHAR(255) NOT NULL,
    phone        VARCHAR(25),
    street       VARCHAR(255) NOT NULL,
    city         VARCHAR(100) NOT NULL,
    state        CHAR(2) NOT NULL,
    zip_code     VARCHAR(10) NOT NULL,
    UNIQUE KEY uk_customers_email (email)
) ENGINE=InnoDB;
CREATE TABLE products (
    product_id   INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    brand_id     INT NOT NULL,
    category_id  INT NOT NULL,
    model_year   SMALLINT NOT NULL,
    list_price   DECIMAL(10,2) NOT NULL,
    CONSTRAINT fk_products_brand
        FOREIGN KEY (brand_id) REFERENCES brands (brand_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT fk_products_category
        FOREIGN KEY (category_id) REFERENCES categories (category_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    INDEX idx_products_brand (brand_id),
    INDEX idx_products_category (category_id)
) ENGINE=InnoDB;
CREATE TABLE staffs (
    staff_id    INT AUTO_INCREMENT PRIMARY KEY,
    first_name  VARCHAR(100) NOT NULL,
    last_name   VARCHAR(100) NOT NULL,
    email       VARCHAR(255) NOT NULL,
    phone       VARCHAR(25),
    active      TINYINT(1) NOT NULL DEFAULT 1,
    street      VARCHAR(255) NOT NULL,
    store_id    INT NOT NULL,
    manager_id  INT NULL,
    UNIQUE KEY uk_staffs_email (email),
    INDEX idx_staffs_store (store_id),
    CONSTRAINT fk_staffs_store
        FOREIGN KEY (store_id) REFERENCES stores (store_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT fk_staffs_manager
        FOREIGN KEY (manager_id) REFERENCES staffs (staff_id)
        ON UPDATE CASCADE
        ON DELETE SET NULL
) ENGINE=InnoDB;
CREATE TABLE stocks (
    store_id    INT NOT NULL,
    product_id  INT NOT NULL,
    quantity    INT NOT NULL DEFAULT 0,
    PRIMARY KEY (store_id, product_id),
    CONSTRAINT fk_stocks_store
        FOREIGN KEY (store_id) REFERENCES stores (store_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT fk_stocks_product
        FOREIGN KEY (product_id) REFERENCES products (product_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
) ENGINE=InnoDB;
-- Sales activity ----------------------------------------------------------
CREATE TABLE orders (
    order_id       INT PRIMARY KEY,
    customer_id    INT NOT NULL,
    store_id       INT NOT NULL,
    staff_id       INT NOT NULL,
    order_status   TINYINT NOT NULL,
    order_date     DATE NOT NULL,
    required_date  DATE NOT NULL,
    shipped_date   DATE NULL,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT fk_orders_store
        FOREIGN KEY (store_id) REFERENCES stores (store_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    CONSTRAINT fk_orders_staff
        FOREIGN KEY (staff_id) REFERENCES staffs (staff_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    INDEX idx_orders_customer (customer_id),
    INDEX idx_orders_store (store_id),
    INDEX idx_orders_staff (staff_id)
) ENGINE=InnoDB;
CREATE TABLE order_items (
    order_id    INT NOT NULL,
    item_id     INT NOT NULL,
    product_id  INT NOT NULL,
    quantity    INT NOT NULL,
    list_price  DECIMAL(10,2) NOT NULL,
    discount    DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    PRIMARY KEY (order_id, item_id),
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES orders (order_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id) REFERENCES products (product_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
) ENGINE=InnoDB;
-- Helpful views -----------------------------------------------------------
CREATE OR REPLACE VIEW vw_order_details AS
SELECT
    o.order_id,
    o.order_date,
    c.first_name AS customer_first_name,
    c.last_name  AS customer_last_name,
    s.store_name,
    CONCAT(sf.first_name, ' ', sf.last_name) AS staff_member,
    oi.item_id,
    p.product_name,
    oi.quantity,
    oi.list_price,
    oi.discount,
    (oi.quantity * oi.list_price * (1 - oi.discount)) AS line_total
FROM orders AS o
JOIN customers AS c   ON c.customer_id = o.customer_id
JOIN stores    AS s   ON s.store_id = o.store_id
JOIN staffs    AS sf  ON sf.staff_id = o.staff_id
JOIN order_items AS oi ON oi.order_id = o.order_id
JOIN products AS p ON p.product_id = oi.product_id;