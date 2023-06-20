-- App Hashes: https://dune.com/queries/1610025
-- MIN(first_block_seen) = 12153263
-- Nov 16, 2022: Query takes 4 seconds to run for on full block range
with
app_hashes as (
    select
        min(call_block_number) first_seen_block,
        get_json_object(trade, '$.appData') as app_hash
    from gnosis_protocol_v2_ethereum.GPv2Settlement_call_settle
        lateral view explode(trades) as trade
    group by app_hash
)
select
    app_hash,
    first_seen_block
from app_hashes
where first_seen_block > '{{BlockFrom}}'
and first_seen_block <= '{{BlockTo}}'

-- For some additional stats,
-- on this data see https://dune.com/queries/1608286