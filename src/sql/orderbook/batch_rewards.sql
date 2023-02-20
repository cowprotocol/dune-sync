WITH hashed_observations AS (
SELECT
    -- settlement
    tx_hash,
    -- settlement_observations
    block_number,
    effective_gas_price * gas_used AS execution_cost,
    surplus,
    fee,
    -- auction_transaction
    auction_id
FROM settlement_observations so
JOIN settlements s
  ON s.block_number = so.block_number
  AND s.log_index = so.log_index
JOIN auction_transaction at
  ON s.tx_from = at.tx_from
  AND s.tx_nonce = at.tx_nonce
WHERE block_number > {{start_block}} AND block_number <= {{end_block}}
),

reward_data AS (
  SELECT
     -- observations
     block_number,
     tx_hash,
     execution_cost,
     surplus,
     fee,
     surplus + fee - reference_score AS reward_eth,
     -- scores
     winning_score,
     reference_score,
     -- participation
     participants
FROM hashed_observations ho
-- outer joins made in order to detect missing data.
LEFT OUTER JOIN settlement_scores ss
  ON ho.auction_id = ss.auction_id
LEFT OUTER JOIN auction_participants ap
  ON ho.auction_id = ap.auction_id
)

SELECT * FROM reward_data
