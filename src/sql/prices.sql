-- Selects all prices collected in the analytics db
SELECT 
  concat('0x', encode(token_address, 'hex')) as token_address,
  time,
  price,
  source
FROM prices
