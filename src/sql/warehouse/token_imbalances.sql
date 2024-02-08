select
     block_number,
     concat('0x', encode(s.tx_hash, 'hex')) as tx_hash,
     concat('0x', encode(token, 'hex')) as token,
     amount :: text as amount
from
     internalized_imbalances
     join settlements s on internalized_imbalances.tx_hash = s.tx_hash
where
     block_number > {{start_block}}
     and block_number <= {{end_block}};