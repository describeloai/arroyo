CREATE TABLE impulse WITH (
    connector = 'impulse',
    event_rate = '10'
);

CREATE TABLE orders (
    k BIGINT
) WITH (
    connector = 'metalake',
    'catalog.postgres' = 'host=localhost user=postgres',
    'catalog.schema' = 'lake',
    namespace = 'shop',
    table = 'orders',
    type = 'sink'
);

INSERT INTO orders SELECT counter FROM impulse;
