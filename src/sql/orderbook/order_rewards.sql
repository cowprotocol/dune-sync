with trade_hashes as (
    SELECT
        settlement.solver,
        t.block_number as block_number,
        order_uid,
        fee_amount,
        settlement.tx_hash,
        auction_id
    FROM
        trades t
        LEFT OUTER JOIN LATERAL (
            SELECT
                tx_hash,
                solver,
                tx_nonce,
                tx_from,
                auction_id,
                block_number,
                log_index
            FROM
                settlements s
            WHERE
                s.block_number = t.block_number
                AND s.log_index > t.log_index
            ORDER BY
                s.log_index ASC
            LIMIT
                1
        ) AS settlement ON true
        join settlement_observations so on settlement.block_number = so.block_number
        and settlement.log_index = so.log_index
    where
        settlement.block_number > {{start_block}}
        and settlement.block_number <= {{end_block}}
),
order_surplus AS (
    SELECT
        s.auction_id,
        t.order_uid,
        o.sell_token,
        o.buy_token,
        t.sell_amount, -- the total amount the user sends
        t.buy_amount, -- the total amount the user receives
        oe.surplus_fee as observed_fee, -- the total discrepancy between what the user sends and what they would have send if they traded at clearing price
        o.kind,
        CASE
            WHEN o.kind = 'sell' THEN t.buy_amount - t.sell_amount * o.buy_amount / (o.sell_amount + o.fee_amount)
            WHEN o.kind = 'buy' THEN t.buy_amount * (o.sell_amount + o.fee_amount) / o.buy_amount - t.sell_amount
        END AS surplus,
        CASE
            WHEN o.kind = 'sell' THEN o.buy_token
            WHEN o.kind = 'buy' THEN o.sell_token
        END AS surplus_token
    FROM
        settlements s -- links block_number and log_index to tx_from and tx_nonce
        JOIN settlement_scores ss -- contains block_deadline
        ON s.auction_id = ss.auction_id
        JOIN trades t -- contains traded amounts
        ON s.block_number = t.block_number -- log_index cannot be checked, does not work correctly with multiple auctions on the same block
        JOIN orders o -- contains tokens and limit amounts
        ON t.order_uid = o.uid
        JOIN order_execution oe -- contains surplus fee
        ON t.order_uid = oe.order_uid
        AND s.auction_id = oe.auction_id
    WHERE
        s.block_number > {{start_block}}
        AND s.block_number <= {{end_block}}
),
order_protocol_fee AS (
    SELECT
        os.auction_id,
        os.order_uid,
        os.sell_amount,
        os.buy_amount,
        os.sell_token,
        os.observed_fee,
        os.surplus,
        os.surplus_token,
        CASE
            WHEN fp.kind = 'surplus' THEN CASE
                WHEN os.kind = 'sell' THEN
                -- We assume that the case surplus_factor != 1 always. In
                -- that case reconstructing the protocol fee would be
                -- impossible anyways. This query will return a division by
                -- zero error in that case.
                LEAST(
                    fp.max_volume_factor * os.sell_amount * os.buy_amount / (os.sell_amount - os.observed_fee), -- at most charge a fraction of volume
                    fp.surplus_factor / (1 - fp.surplus_factor) * surplus -- charge a fraction of surplus
                )
                WHEN os.kind = 'buy' THEN LEAST(
                    fp.max_volume_factor / (1 + fp.max_volume_factor) * os.sell_amount, -- at most charge a fraction of volume
                    fp.surplus_factor / (1 - fp.surplus_factor) * surplus -- charge a fraction of surplus
                )
            END
            WHEN fp.kind = 'volume' THEN fp.volume_factor / (1 - fp.volume_factor) * os.sell_amount
        END AS protocol_fee,
        CASE
            WHEN fp.kind = 'surplus' THEN os.surplus_token
            WHEN fp.kind = 'volume' THEN os.sell_token
        END AS protocol_fee_token
    FROM
        order_surplus os
        JOIN fee_policies fp -- contains protocol fee policy
        ON os.auction_id = fp.auction_id
        AND os.order_uid = fp.order_uid
),
order_protocol_fee_prices AS (
    SELECT
        opf.order_uid,
        opf.auction_id,
        opf.protocol_fee,
        opf.protocol_fee_token,
        ap.price / pow(10, 18) as protocol_fee_native_price
    FROM
        order_protocol_fee opf
        JOIN auction_prices ap -- contains price: protocol fee token
        ON opf.auction_id = ap.auction_id
        AND opf.protocol_fee_token = ap.token
),
winning_quotes as (
    SELECT
        concat('0x', encode(oq.solver, 'hex')) as quote_solver,
        oq.order_uid
    FROM
        trades t
        INNER JOIN orders o ON order_uid = uid
        JOIN order_quotes oq ON t.order_uid = oq.order_uid
    WHERE
        (
            o.class = 'market'
            OR (
                o.kind = 'sell'
                AND (
                    oq.sell_amount - oq.gas_amount * oq.gas_price / oq.sell_token_price
                ) * oq.buy_amount >= o.buy_amount * oq.sell_amount
            )
            OR (
                o.kind = 'buy'
                AND o.sell_amount >= oq.sell_amount + oq.gas_amount * oq.gas_price / oq.sell_token_price
            )
        )
        AND o.partially_fillable = 'f' -- the code above might fail for partially fillable orders
        AND t.block_number > { { start_block } }
        AND t.block_number <= { { end_block } }
        AND oq.solver != '\x0000000000000000000000000000000000000000'
) -- Most efficient column order for sorting would be having tx_hash or order_uid first
select
    trade_hashes.block_number as block_number,
    concat('0x', encode(trade_hashes.order_uid, 'hex')) as order_uid,
    concat('0x', encode(solver, 'hex')) as solver,
    quote_solver,
    concat('0x', encode(tx_hash, 'hex')) as tx_hash,
    coalesce(surplus_fee, 0) :: text as surplus_fee,
    coalesce(reward, 0.0) as amount,
    coalesce(cast(protocol_fee as numeric(78, 0)), 0) :: text as protocol_fee,
    CASE
        WHEN protocol_fee_token is not NULL THEN concat('0x', encode(protocol_fee_token, 'hex'))
    END as protocol_fee_token,
    coalesce(protocol_fee_native_price, 0.0) as protocol_fee_native_price
from
    trade_hashes
    left outer join order_execution o on trade_hashes.order_uid = o.order_uid
    and trade_hashes.auction_id = o.auction_id
    left outer join winning_quotes wq on trade_hashes.order_uid = wq.order_uid
    left outer join order_protocol_fee_prices opfp on trade_hashes.order_uid = opfp.order_uid
    and trade_hashes.auction_id = opfp.auction_id;