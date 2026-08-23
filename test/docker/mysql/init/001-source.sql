CREATE DATABASE IF NOT EXISTS source_db
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

CREATE USER IF NOT EXISTS 'seatunnel'@'%' IDENTIFIED BY 'seatunnel_pw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'seatunnel'@'%';
FLUSH PRIVILEGES;

USE source_db;
SET NAMES utf8mb4;

CREATE TABLE customers (
    id BIGINT NOT NULL,
    email VARCHAR(255) NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    credit DECIMAL(12,2) NOT NULL DEFAULT 0,
    profile JSON NULL,
    notes TEXT NULL,
    registered_at DATETIME(6) NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE KEY uk_customers_email (email)
) ENGINE=InnoDB;

CREATE TABLE orders (
    tenant_id INT NOT NULL,
    order_no BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    amount DECIMAL(14,2) NOT NULL,
    order_status VARCHAR(20) NOT NULL,
    ordered_at DATETIME(6) NOT NULL,
    PRIMARY KEY (tenant_id, order_no)
) ENGINE=InnoDB;

CREATE TABLE inventory_by_sku (
    sku VARCHAR(64) NOT NULL,
    warehouse_code VARCHAR(32) NOT NULL,
    quantity INT NOT NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    UNIQUE KEY uk_inventory_sku_warehouse (sku, warehouse_code)
) ENGINE=InnoDB;

INSERT INTO customers
    (id, email, display_name, active, credit, profile, notes, registered_at)
VALUES
    (1, 'alice@example.com', 'Alice 😀', 1, 120.50, JSON_OBJECT('tier', 'gold', 'locale', 'zh-CN'), 'initial customer', '2026-08-20 01:00:00.123456'),
    (2, 'bob@example.com', 'Bob', 1, 42.00, JSON_OBJECT('tier', 'silver'), NULL, '2026-08-20 01:05:00.000000');

INSERT INTO orders
    (tenant_id, order_no, customer_id, amount, order_status, ordered_at)
VALUES
    (10, 1001, 1, 88.80, 'CREATED', '2026-08-20 01:10:00.000000'),
    (10, 1002, 2, 19.90, 'PAID', '2026-08-20 01:11:00.000000'),
    (20, 2001, 1, 256.00, 'CREATED', '2026-08-20 01:12:00.000000');

INSERT INTO inventory_by_sku (sku, warehouse_code, quantity)
VALUES
    ('SKU-001', 'WH-A', 100),
    ('SKU-001', 'WH-B', 50);
