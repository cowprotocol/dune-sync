-- Selects all known appData hashes and preimages (as string) from the backend database

SELECT 
  concat('0x', encode(token_address, 'hex')) as token_address,
  time,
  price,
  source
FROM prices
