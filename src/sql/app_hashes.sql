-- Selects all known appData hashes and preimages (as string) from the backend database

SELECT 
  concat('0x',encode(contract_app_data, 'hex')) contract_app_data, 
  encode(full_app_data, 'escape')
FROM app_data
