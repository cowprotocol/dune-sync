with trade_hashes as (SELECT settlement.solver,
                             block_number,
                             order_uid,
                             fee_amount,
                             settlement.tx_hash,
                             auction_id
                      FROM trades t
                             LEFT OUTER JOIN LATERAL (
                        SELECT tx_hash, solver, tx_nonce, tx_from
                        FROM settlements s
                        WHERE s.block_number = t.block_number
                          AND s.log_index > t.log_index
                        ORDER BY s.log_index ASC
                        LIMIT 1
                        ) AS settlement ON true
                             join auction_transaction
                        -- This join also eliminates overlapping
                        -- trades & settlements between barn and prod DB
                                  on settlement.tx_from = auction_transaction.tx_from
                                    and settlement.tx_nonce = auction_transaction.tx_nonce
                      where block_number > {{start_block}} and block_number <= {{end_block}}),

     winning_quotes as (SELECT concat('0x', encode(oq.solver, 'hex')) as quote_solver,
                               oq.order_uid
                        FROM trades t
                               INNER JOIN orders o ON order_uid = uid
                               JOIN order_quotes oq ON t.order_uid = oq.order_uid
                        WHERE block_number > {{start_block}}
                          AND block_number <= {{end_block}}
                          AND oq.solver != '\x0000000000000000000000000000000000000000')

-- Most efficient column order for sorting would be having tx_hash or order_uid first
select block_number,
       concat('0x', encode(trade_hashes.order_uid, 'hex')) as order_uid,
       concat('0x', encode(solver, 'hex'))                 as solver,
       quote_solver,
       concat('0x', encode(tx_hash, 'hex'))                as tx_hash,
       coalesce(surplus_fee, 0)::text                      as surplus_fee,
       coalesce(reward, 0.0)                               as amount
from trade_hashes
       left outer join order_execution o
                       on trade_hashes.order_uid = o.order_uid
                         and trade_hashes.auction_id = o.auction_id
      left outer join winning_quotes wq
            on o.order_uid = wq.order_uid;
