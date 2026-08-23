USE source_db;
SET NAMES utf8mb4;

UPDATE customers
SET display_name = 'Alice Updated 😀', credit = 199.99,
    profile = JSON_OBJECT('tier', 'platinum', 'locale', 'zh-CN')
WHERE id = 1;

DELETE FROM customers WHERE id = 2;

INSERT INTO customers
    (id, email, display_name, active, credit, profile, notes, registered_at)
VALUES
    (3, 'carol@example.com', 'Carol', 1, 66.60, JSON_OBJECT('tier', 'gold'), 'created by CDC case', '2026-08-20 02:00:00.654321')
ON DUPLICATE KEY UPDATE
    email = VALUES(email),
    display_name = VALUES(display_name),
    active = VALUES(active),
    credit = VALUES(credit),
    profile = VALUES(profile),
    notes = VALUES(notes),
    registered_at = VALUES(registered_at);

UPDATE orders
SET order_status = 'PAID', amount = 99.90
WHERE tenant_id = 10 AND order_no = 1001;

DELETE FROM orders
WHERE tenant_id = 10 AND order_no = 1002;

INSERT INTO orders
    (tenant_id, order_no, customer_id, amount, order_status, ordered_at)
VALUES
    (20, 2002, 3, 35.50, 'CREATED', '2026-08-20 02:05:00.000000')
ON DUPLICATE KEY UPDATE
    customer_id = VALUES(customer_id),
    amount = VALUES(amount),
    order_status = VALUES(order_status),
    ordered_at = VALUES(ordered_at);
