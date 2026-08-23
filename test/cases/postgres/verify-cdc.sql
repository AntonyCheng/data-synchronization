DO $$
BEGIN
    IF (SELECT count(*) FROM public.customers) <> 2 THEN
        RAISE EXCEPTION 'expected 2 customers after CDC changes';
    END IF;

    IF EXISTS (SELECT 1 FROM public.customers WHERE id = 2) THEN
        RAISE EXCEPTION 'customer delete was not propagated';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.customers
        WHERE id = 1 AND display_name = 'Alice Updated 😀' AND credit = 199.99
    ) THEN
        RAISE EXCEPTION 'customer update was not propagated';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.customers
        WHERE id = 3 AND email = 'carol@example.com'
    ) THEN
        RAISE EXCEPTION 'customer insert was not propagated';
    END IF;

    IF (SELECT count(*) FROM public.orders) <> 3 THEN
        RAISE EXCEPTION 'expected 3 orders after CDC changes';
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.orders
        WHERE tenant_id = 10 AND order_no = 1002
    ) THEN
        RAISE EXCEPTION 'composite-key delete was not propagated';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.orders
        WHERE tenant_id = 10 AND order_no = 1001
          AND order_status = 'PAID' AND amount = 99.90
    ) THEN
        RAISE EXCEPTION 'composite-key update was not propagated';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM public.orders
        WHERE tenant_id = 20 AND order_no = 2002
    ) THEN
        RAISE EXCEPTION 'composite-key insert was not propagated';
    END IF;
END $$;
