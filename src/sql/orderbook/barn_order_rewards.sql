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
        ss.winner as solver,
        s.auction_id,
        s.tx_hash,
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
            WHEN o.kind = 'sell' THEN t.buy_amount - t.sell_amount * (oq.buy_amount - oq.buy_amount / oq.sell_amount * oq.gas_amount * oq.gas_price / oq.sell_token_price) / oq.sell_amount
            WHEN o.kind = 'buy' THEN t.buy_amount * (oq.sell_amount + oq.gas_amount * oq.gas_price / oq.sell_token_price) / oq.buy_amount - t.sell_amount
        END AS price_improvement,
        CASE
            WHEN o.kind = 'sell' THEN o.buy_token
            WHEN o.kind = 'buy' THEN o.sell_token
        END AS surplus_token,
        ad.full_app_data as app_data
    FROM
        settlements s
        JOIN settlement_scores ss -- contains block_deadline
        ON s.auction_id = ss.auction_id
        JOIN trades t -- contains traded amounts
        ON s.block_number = t.block_number -- log_index cannot be checked, does not work correctly with multiple auctions on the same block
        JOIN orders o -- contains tokens and limit amounts
        ON t.order_uid = o.uid
        JOIN order_execution oe -- contains surplus fee
        ON t.order_uid = oe.order_uid
        AND s.auction_id = oe.auction_id
        LEFT OUTER JOIN order_quotes oq -- contains quote amounts
        ON o.uid = oq.order_uid
        LEFT OUTER JOIN app_data ad -- contains full app data
        on o.app_data = ad.contract_app_data
    WHERE
        ss.block_deadline >= {{start_block}}
        -- since this table filtered on block_deadline is joined with another table filtered on block_number
        -- the bound for this table need to be a bit looser.
        AND ss.block_deadline <= {{end_block}} + 100
),
fee_policies_first_proxy as (
    select
        auction_id,
        order_uid,
        max(application_order) as application_order,
        count (*) as num_policies
    from fee_policies
    where auction_id in (select auction_id from order_surplus)
    group by order_uid, auction_id
),
fee_policies_first as (
    select
        fp.auction_id,
        fp.order_uid,
        fp.application_order,
        fp.kind,
        fp.surplus_factor,
        fp.surplus_max_volume_factor,
        fp.volume_factor,
        fp.price_improvement_factor,
        fp.price_improvement_max_volume_factor
    from fee_policies_first_proxy fpmp join fee_policies fp on fp.auction_id = fpmp.auction_id and fp.order_uid = fpmp.order_uid and fp.application_order = fpmp.application_order
),
fee_policies_temp as (
    select
        *
    from fee_policies
    where auction_id in (select auction_id from order_surplus)
    except (select * from fee_policies_first )
),
fee_policies_second as (
    select
        *
    from fee_policies_temp
    UNION
    select
        auction_id,
        order_uid,
        0 as application_order,
        'volume' as kind,
        null as surplus_factor,
        null as surplus_max_volume_factor,
        0 as volume_factor,
        null as price_improvement_factor,
        null as price_improvement_max_volume_factor
    from fee_policies_first_proxy where num_policies = 1
),
order_protocol_fee_first AS (
    SELECT
        os.auction_id,
        os.order_uid,
        os.sell_amount,
        os.buy_amount,
        os.surplus,
        os.price_improvement,
        os.kind,
        convert_from(os.app_data, 'UTF8')::JSONB->'metadata'->'partnerFee'->>'recipient' as partner_fee_recipient,
        fp.kind as protocol_fee_kind_first,
        CASE
            WHEN fp.kind = 'surplus' THEN CASE
                WHEN os.kind = 'sell' THEN
                -- We assume that the case surplus_factor != 1 always. In
                -- that case reconstructing the protocol fee would be
                -- impossible anyways. This query will return a division by
                -- zero error in that case.
                LEAST(
                    fp.surplus_max_volume_factor / (1 - fp.surplus_max_volume_factor) * os.buy_amount,
                    -- at most charge a fraction of volume
                    fp.surplus_factor / (1 - fp.surplus_factor) * surplus -- charge a fraction of surplus
                )
                WHEN os.kind = 'buy' THEN LEAST(
                    fp.surplus_max_volume_factor / (1 + fp.surplus_max_volume_factor) * os.sell_amount,
                    -- at most charge a fraction of volume
                    fp.surplus_factor / (1 - fp.surplus_factor) * surplus -- charge a fraction of surplus
                )
            END
            WHEN fp.kind = 'priceimprovement' THEN CASE
                WHEN os.kind = 'sell' THEN
                LEAST(
                    -- at most charge a fraction of volume
                    fp.price_improvement_max_volume_factor / (1 - fp.price_improvement_max_volume_factor) * os.buy_amount,
                    -- charge a fraction of price improvement, at most 0
                    GREATEST(
                        fp.price_improvement_factor / (1 - fp.price_improvement_factor) * price_improvement
                        ,
                        0
                    )
                )
                WHEN os.kind = 'buy' THEN LEAST(
                    -- at most charge a fraction of volume
                    fp.price_improvement_max_volume_factor / (1 + fp.price_improvement_max_volume_factor) * os.sell_amount,
                    -- charge a fraction of price improvement
                    GREATEST(
                        fp.price_improvement_factor / (1 - fp.price_improvement_factor) * price_improvement,
                        0
                    )
                )
            END
            WHEN fp.kind = 'volume' THEN CASE
                WHEN os.kind = 'sell' THEN
                    fp.volume_factor / (1 - fp.volume_factor) * os.buy_amount
                WHEN os.kind = 'buy' THEN
                    fp.volume_factor / (1 + fp.volume_factor) * os.sell_amount
            END
        END AS protocol_fee_first,
        os.surplus_token AS protocol_fee_token
    FROM
        order_surplus os
        JOIN fee_policies_first fp -- contains protocol fee policy
        ON os.auction_id = fp.auction_id
        AND os.order_uid = fp.order_uid
),
order_surplus_intermediate as (
    select
        auction_id,
        order_uid,
        CASE
            WHEN kind = 'sell' then sell_amount
            ELSE sell_amount - protocol_fee_first
        END as sell_amount,
        CASE
            WHEN kind = 'sell' then buy_amount + protocol_fee_first
            ELSE buy_amount
        END as buy_amount,
        surplus + protocol_fee_first as surplus,
        price_improvement + protocol_fee_first as price_improvement,
        protocol_fee_kind_first,
        protocol_fee_first,
        partner_fee_recipient
    from order_protocol_fee_first
),
order_protocol_fee as (
    SELECT
        os.auction_id,
        os.solver,
        os.tx_hash,
        os.order_uid,
        os.sell_amount,
        os.buy_amount,
        os.sell_token,
        os.observed_fee,
        os.surplus,
        os.surplus_token,
        protocol_fee_kind_first,
        fp.kind as protocol_fee_kind_second,
        protocol_fee_first,
        CASE
            WHEN fp.kind = 'surplus' THEN CASE
                WHEN os.kind = 'sell' THEN
                -- We assume that the case surplus_factor != 1 always. In
                -- that case reconstructing the protocol fee would be
                -- impossible anyways. This query will return a division by
                -- zero error in that case.
                protocol_fee_first + LEAST(
                    fp.surplus_max_volume_factor / (1 - fp.surplus_max_volume_factor) * osi.buy_amount,
                    -- at most charge a fraction of volume
                    fp.surplus_factor / (1 - fp.surplus_factor) * osi.surplus -- charge a fraction of surplus
                )
                WHEN os.kind = 'buy' THEN protocol_fee_first + LEAST(
                    fp.surplus_max_volume_factor / (1 + fp.surplus_max_volume_factor) * osi.sell_amount,
                    -- at most charge a fraction of volume
                    fp.surplus_factor / (1 - fp.surplus_factor) * osi.surplus -- charge a fraction of surplus
                )
            END
            WHEN fp.kind = 'priceimprovement' THEN CASE
                WHEN os.kind = 'sell' THEN
                protocol_fee_first + LEAST(
                    -- at most charge a fraction of volume
                    fp.price_improvement_max_volume_factor / (1 - fp.price_improvement_max_volume_factor) * osi.buy_amount,
                    -- charge a fraction of price improvement, at most 0
                    GREATEST(
                        fp.price_improvement_factor / (1 - fp.price_improvement_factor) * osi.price_improvement
                        ,
                        0
                    )
                )
                WHEN os.kind = 'buy' THEN protocol_fee_first + LEAST(
                    -- at most charge a fraction of volume
                    fp.price_improvement_max_volume_factor / (1 + fp.price_improvement_max_volume_factor) * osi.sell_amount,
                    -- charge a fraction of price improvement
                    GREATEST(
                        fp.price_improvement_factor / (1 - fp.price_improvement_factor) * osi.price_improvement,
                        0
                    )
                )
            END
            WHEN fp.kind = 'volume' THEN CASE
                WHEN os.kind = 'sell' THEN
                    protocol_fee_first + fp.volume_factor / (1 - fp.volume_factor) * osi.buy_amount
                WHEN os.kind = 'buy' THEN
                    protocol_fee_first + fp.volume_factor / (1 + fp.volume_factor) * osi.sell_amount
            END
        END AS protocol_fee,
        osi.partner_fee_recipient,
        CASE
            WHEN osi.partner_fee_recipient IS NOT NULL THEN osi.protocol_fee_first
            ELSE 0
        END AS partner_fee,
        os.surplus_token AS protocol_fee_token
    FROM
        order_surplus os
        JOIN order_surplus_intermediate osi
        ON os.order_uid = osi.order_uid AND os.auction_id = osi.auction_id
        JOIN fee_policies_second fp -- contains protocol fee policy
        ON os.auction_id = fp.auction_id
        AND os.order_uid = fp.order_uid
),
order_protocol_fee_prices AS (
    SELECT
        opf.auction_id,
        opf.solver,
        opf.tx_hash,
        opf.order_uid,
        opf.surplus,
        opf.protocol_fee,
        opf.protocol_fee_token,
        CASE
            WHEN opf.partner_fee_recipient IS NOT NULL THEN opf.protocol_fee_kind_second
            ELSE opf.protocol_fee_kind_first
        END AS protocol_fee_kind,
        opf.partner_fee,
        opf.partner_fee_recipient,
        CASE
            WHEN opf.sell_token != opf.protocol_fee_token THEN (opf.sell_amount - opf.observed_fee) / opf.buy_amount * opf.protocol_fee
            ELSE opf.protocol_fee
        END AS network_fee_correction,
        opf.sell_token as network_fee_token,
        ap_surplus.price / pow(10, 18) as surplus_token_native_price,
        ap_protocol.price / pow(10, 18) as protocol_fee_token_native_price,
        ap_sell.price / pow(10, 18) as network_fee_token_native_price
    FROM
        order_protocol_fee as opf
        JOIN auction_prices ap_sell -- contains price: sell token
        ON opf.auction_id = ap_sell.auction_id
        AND opf.sell_token = ap_sell.token
        JOIN auction_prices ap_surplus -- contains price: surplus token
        ON opf.auction_id = ap_surplus.auction_id
        AND opf.surplus_token = ap_surplus.token
        JOIN auction_prices ap_protocol -- contains price: protocol fee token
        ON opf.auction_id = ap_protocol.auction_id
        AND opf.protocol_fee_token = ap_protocol.token
),
winning_quotes as (
    SELECT
        case
            when oq.solver='\x9DFc9Bb0FfF2dc96728D2bb94eaCee6ba3592351' then concat('0x', encode('\x26B5e3bF135D3Dd05A220508dD61f25BF1A47cBD', 'hex'))
            else concat('0x', encode(oq.solver, 'hex'))
        end as quote_solver,
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
        AND t.block_number > {{start_block}}
        AND t.block_number <= {{end_block}}
        AND oq.solver != '\x0000000000000000000000000000000000000000'
) -- Most efficient column order for sorting would be having tx_hash or order_uid first
select
    trade_hashes.block_number as block_number,
    concat('0x', encode(trade_hashes.order_uid, 'hex')) as order_uid,
    concat('0x', encode(oq.solver, 'hex')) as solver,
    quote_solver,
    concat('0x', encode(trade_hashes.tx_hash, 'hex')) as tx_hash,
    coalesce(surplus_fee, 0) :: text as surplus_fee,
    coalesce(reward, 0.0) as amount,
    coalesce(cast(protocol_fee as numeric(78, 0)), 0) :: text as protocol_fee,
    CASE
        WHEN protocol_fee_token is not NULL THEN concat('0x', encode(protocol_fee_token, 'hex'))
    END as protocol_fee_token,
    coalesce(protocol_fee_token_native_price, 0.0) as protocol_fee_native_price,
    cast(oq.sell_amount as numeric(78, 0)) :: text  as quote_sell_amount,
    cast(oq.buy_amount as numeric(78, 0)) :: text as quote_buy_amount,
    oq.gas_amount * oq.gas_price as quote_gas_cost,
    oq.sell_token_price as quote_sell_token_price,
    cast(coalesce(opfp.partner_fee, 0) as numeric(78, 0)) :: text as partner_fee,
    opfp.partner_fee_recipient,
    opfp.protocol_fee_kind
from
    trade_hashes
    left outer join order_execution o on trade_hashes.order_uid = o.order_uid
    and trade_hashes.auction_id = o.auction_id
    left outer join winning_quotes wq on trade_hashes.order_uid = wq.order_uid
    left outer join order_protocol_fee_prices opfp on trade_hashes.order_uid = opfp.order_uid
    and trade_hashes.auction_id = opfp.auction_id
    left outer join order_quotes oq on trade_hashes.order_uid = oq.order_uid
where
    trade_hashes.tx_hash != '\x7087eb55854228a30c864a9ee4d6c4072d37d53bf4d0404f1064c5b33b7aa96d'
    AND trade_hashes.tx_hash != '\x84eb7aef07139e9558f08ac92b857b727f64c0f44d92a572078f45b7d77ebe74'
