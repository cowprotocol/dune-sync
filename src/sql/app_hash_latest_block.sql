-- https://dune.com/queries/1615490
select
  max(call_block_number) as latest_block
from gnosis_protocol_v2_ethereum.GPv2Settlement_call_settle