CREATE TABLE orders (
    id BIGINT,
    amount DOUBLE
) WITH (
    connector = 'metalake',
    'catalog.postgres' = 'host=localhost user=postgres',
    'catalog.schema' = 'lake',
    namespace = 'shop',
    table = 'orders',
    type = 'source'
);

SELECT id, count(*), sum(amount)
FROM orders
GROUP BY id;
