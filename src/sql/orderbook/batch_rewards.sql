with
hashed_observations as (
  SELECT
    block_number,
    tx_hash,
    effective_gas_price * gas_used as execution_cost,
    surplus,
    fee,
    auction_id
  FROM settlement_observations so
  LEFT OUTER JOIN LATERAL (
    SELECT tx_hash, solver, tx_nonce, tx_from
    FROM settlements s
    WHERE s.block_number = so.block_number
      AND s.log_index > so.log_index
    ORDER BY s.log_index
    LIMIT 1
  ) AS settlement
    ON TRUE
  JOIN auction_transaction
     ON settlement.tx_from = auction_transaction.tx_from
    AND settlement.tx_nonce = auction_transaction.tx_nonce
  WHERE block_number > {{start_block}} AND block_number <= {{end_block}}
)

select
    -- observations
    block_number,
    tx_hash,
    execution_cost,
    surplus,
    fee,
    surplus + fee - reference_score as reward_eth,
    -- scores
    winning_score,
    reference_score,
    -- participation
    participants
from hashed_observations ho
-- may want to do outer joins
join settlement_scores ss
  on ho.auction_id = ss.auction_id
-- may want to do outer joins
join auction_participants ap
  on ho.auction_id = ap.auction_id
