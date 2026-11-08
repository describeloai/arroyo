--fail=input is updating, but sink is not configured as an updating sink
CREATE TABLE changes (
    k BIGINT
) WITH (
    connector = 'metalake',
    'catalog.postgres' = 'host=localhost user=postgres',
    'catalog.schema' = 'lake',
    namespace = 'shop',
    table = 'changes',
    type = 'source'
);

CREATE TABLE totals (
    k BIGINT
) WITH (
    connector = 'metalake',
    'catalog.postgres' = 'host=localhost user=postgres',
    'catalog.schema' = 'lake',
    namespace = 'shop',
    table = 'totals',
    type = 'sink'
);

INSERT INTO totals SELECT count(*) FROM changes;
