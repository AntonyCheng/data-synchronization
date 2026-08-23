DO $$
BEGIN
    IF (SELECT count(*) FROM public.customers) <> 2 THEN
        RAISE EXCEPTION 'expected 2 initial customers';
    END IF;

    IF (SELECT count(*) FROM public.orders) <> 3 THEN
        RAISE EXCEPTION 'expected 3 initial orders';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.customers
        WHERE id = 1 AND display_name = 'Alice 😀' AND credit = 120.50
    ) THEN
        RAISE EXCEPTION 'initial customer content mismatch';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.orders
        WHERE tenant_id = 10 AND order_no = 1001 AND order_status = 'CREATED'
    ) THEN
        RAISE EXCEPTION 'initial composite-key order mismatch';
    END IF;
END $$;
