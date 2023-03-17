WITH observed_settlements AS (SELECT
                                -- settlement
                                tx_hash,
                                solver,
                                s.block_number,
                                -- settlement_observations
                                effective_gas_price * gas_used AS execution_cost,
                                surplus,
                                fee,
                                -- auction_transaction
                                at.auction_id
                              FROM settlement_observations so
                                     JOIN settlements s
                                          ON s.block_number = so.block_number
                                            AND s.log_index = so.log_index
                                     JOIN auction_transaction at
                                          ON s.tx_from = at.tx_from
                                            AND s.tx_nonce = at.tx_nonce
                                     JOIN settlement_scores ss
                                          ON at.auction_id = ss.auction_id
                              WHERE ss.block_deadline > {{start_block}}
                                AND ss.block_deadline <= {{end_block}}),

     auction_participation as (SELECT ss.auction_id,
                                      array_agg(
                                              concat('0x', encode(participant, 'hex')) ORDER BY participant
                                        ) as participating_solvers
                               FROM auction_participants
                                      JOIN settlement_scores ss
                                           ON auction_participants.auction_id = ss.auction_id
                               WHERE block_deadline > {{start_block}}
                                 AND block_deadline <= {{end_block}}
                               GROUP BY ss.auction_id),
     reward_data AS (SELECT
                       -- observations
                       tx_hash,
                       ss.auction_id,
                       -- TODO - Assuming that `solver == winner` when both not null
                       --  We will need to monitor that `solver == winner`!
                       coalesce(solver, winner)               as solver,
                       block_number                           as settlement_block,
                       block_deadline,
                       case
                         when block_number is not null and block_number > block_deadline then 0
                         else coalesce(execution_cost, 0) end as execution_cost,
                       case
                         when block_number is not null and block_number > block_deadline then 0
                         else coalesce(surplus, 0) end        as surplus,
                       case
                         when block_number is not null and block_number > block_deadline then 0
                         else coalesce(fee, 0) end            as fee,
                       -- scores
                       winning_score,
                       reference_score,
                       -- auction_participation
                       participating_solvers
                     FROM settlement_scores ss
                            -- If there are reported scores,
                            -- there will always be a record of auction participants
                            JOIN auction_participation ap
                                 ON ss.auction_id = ap.auction_id
                       -- outer joins made in order to capture non-existent settlements.
                            LEFT OUTER JOIN observed_settlements os
                                            ON os.auction_id = ss.auction_id),
     reward_per_auction as (SELECT tx_hash,
                                   settlement_block,
                                   block_deadline,
                                   solver,
                                   execution_cost,
                                   surplus,
                                   fee,
                                   surplus + fee - reference_score           as uncapped_payment_eth,
                                   -- Uncapped Reward = CLAMP_[-E, E + exec_cost](uncapped_payment_eth)
                                   LEAST(GREATEST(-10000000000000000, surplus + fee - reference_score),
                                         10000000000000000 + execution_cost) as capped_payment,
                                   winning_score,
                                   reference_score,
                                   participating_solvers
                            FROM reward_data)


SELECT settlement_block                    as block_number,
       block_deadline,
       case
         when tx_hash is NULL then NULL
         else concat('0x', encode(tx_hash, 'hex'))
         end                               as tx_hash,
       concat('0x', encode(solver, 'hex')) as solver,
       execution_cost,
       surplus,
       fee,
       uncapped_payment_eth,
       capped_payment,
       winning_score,
       reference_score,
       participating_solvers
FROM reward_per_auction
