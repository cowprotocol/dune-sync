-- Selects all prices collected in the analytics db
SELECT 
  concat('0x', encode(p.token_address, 'hex')) as token_address,
  p.time,
  p.price,
  td.decimals,
  p.source
FROM prices p INNER JOIN token_decimals td ON p.token_address = td.token_address
