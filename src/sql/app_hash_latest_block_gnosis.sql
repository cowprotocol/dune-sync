-- https://dune.com/queries/3865197
select
  max(call_block_number) as latest_block
from
  gnosis_protocol_v2_gnosis.GPv2Settlement_call_settle