select min(block_number) latest
from settlements
where tx_from is null;