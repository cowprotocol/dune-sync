with app_hashes as (select min(block_number) as first_seen_block,
                           concat('0x', encode(orders.app_data, 'hex')) as app_hash
                    from orders
                             inner join trades
                                        on uid = order_uid
                    group by app_data
                    order by first_seen_block)
select *
from app_hashes
where first_seen_block > '{{start_block}}'
and first_seen_block <= '{{end_block}}'