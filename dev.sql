
-- table with declared columns: 
CREATE TABLE purchase_tbl (
    id BIGINT PRIMARY KEY,
    `item_type` VARCHAR,
    `quantity` VARCHAR,
    `price_per_unit` DOUBLE
  ) WITH (
    KAFKA_TOPIC = 'purchases', 
    FORMAT = 'JSON'
  );

SELECT `item_type`, SUM(CAST(`quantity` AS INT)) FROM purchase_tbl GROUP BY `item_type` emit changes;