import unittest

import pandas as pd

from src.fetch.orderbook import OrderbookFetcher
from src.models.block_range import BlockRange


class TestFetchOrderbook(unittest.TestCase):
    def test_latest_block_reasonable(self):
        self.assertGreater(OrderbookFetcher.get_latest_block(), 19184750)

    def test_latest_block_increasing(self):
        latest_block = OrderbookFetcher.get_latest_block()
        self.assertGreaterEqual(OrderbookFetcher.get_latest_block(), latest_block)

    def test_get_order_rewards(self):
        block_number = 19184750
        block_range = BlockRange(block_number, block_number + 25)
        rewards_df = OrderbookFetcher.get_order_rewards(block_range)
        expected = pd.DataFrame(
            {
                "block_number": [
                    19184754,
                    19184754,
                    19184754,
                    19184754,
                    19184754,
                    19184754,
                    19184763,
                ],
                "order_uid": [
                    "0x12228107308040cd64293415c7338b7ec752338aad9738deb9d10bed27cbf81558203dd7b18b262f7a422f75d79cbb230752a74e65c50d9a",
                    "0x05c68d2a038f4feb748fbc7622a3bfb6d2484c56d616e3e3b265d4ad52d177ee40a50cf069e992aa4536211b23f286ef88752187ffffffff",
                    "0x02bffb58877c193873693164a077c7298f470088bc517a77b6eb7d882d5d267e6ce4e9d75a1d6340812284ef564c256061cf10ba65c50cc1",
                    "0x85d3fabb73d7029e0b3eceeeb1fe46a859a5a2aa52a482511950ba633921e63f911da057b7f735bc54e9ffbe1143dda363dba6fb65c50ce2",
                    "0x0d44fb9adc79e104645ddcc4e04b36953e6a37a6da0c2c145350425a09ff9132851064900dcb11351e53a39e5af340e9fc2b768965c50d54",
                    "0xa5d8c049983f2170a37180d01055b92ff67c0db2ccb9cf5d4ec7ee4c095d157f40a50cf069e992aa4536211b23f286ef88752187ffffffff",
                    "0x95e26eba547575e86a81e6e7be87b45b56f2fd824b4bcc573bfb3f7a44b2c3c340a50cf069e992aa4536211b23f286ef88752187ffffffff",
                ],
                "solver": [
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x6f799f4bf6c1c56fb8d890e9e0fff2934b0de157",
                ],
                "quote_solver": [
                    "0xc74b656bd2ebe313d26d1ac02bcf95b137d1c857",
                    "0x16c473448e770ff647c69cbe19e28528877fba1b",
                    None,
                    "0xc74b656bd2ebe313d26d1ac02bcf95b137d1c857",
                    "0x16c473448e770ff647c69cbe19e28528877fba1b",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                ],
                "tx_hash": [
                    "0x43699f7356df831aa04519ecd9f638303f716ab9e41c6ab1f38aacd1c9cdd3e5",
                    "0x43699f7356df831aa04519ecd9f638303f716ab9e41c6ab1f38aacd1c9cdd3e5",
                    "0x43699f7356df831aa04519ecd9f638303f716ab9e41c6ab1f38aacd1c9cdd3e5",
                    "0x43699f7356df831aa04519ecd9f638303f716ab9e41c6ab1f38aacd1c9cdd3e5",
                    "0x43699f7356df831aa04519ecd9f638303f716ab9e41c6ab1f38aacd1c9cdd3e5",
                    "0x43699f7356df831aa04519ecd9f638303f716ab9e41c6ab1f38aacd1c9cdd3e5",
                    "0x321703689c7a2743c85ad91788b8ab00447de42591089d2052a3e9cf3be9d5a1",
                ],
                "surplus_fee": [
                    "0",
                    "0",
                    "57709403",
                    "0",
                    "0",
                    "0",
                    "0",
                ],
                "amount": [
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                ],
                "protocol_fee": [
                    "0",
                    "0",
                    "4382713635683340000",
                    "0",
                    "0",
                    "0",
                    "0",
                ],
                "protocol_fee_token": [
                    None,
                    None,
                    "0xd9fcd98c322942075a5c3860693e9f4f03aae07b",
                    None,
                    None,
                    None,
                    None,
                ],
                "protocol_fee_native_price": [
                    0.0,
                    0.0,
                    0.001552064411669457,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                ],
                "quote_sell_amount": [
                    "4429378850",
                    "184573231326057476",
                    "7500000000",
                    "192465488420922607",
                    "1954467549",
                    "856446990599561516",
                    "71274765128486370400",
                ],
                "quote_buy_amount": [
                    "12009545997505395163",
                    "143919269475547068",
                    "1920099071497969074176",
                    "110185117837140289",
                    "427611951239582233762",
                    "1465179606977149402",
                    "69200861384935994787",
                ],
                "quote_gas_cost": [
                    28980049962721130.0,
                    15426768673942524.0,
                    18282786590639504.0,
                    18696459349125884.0,
                    18629380517476576.0,
                    23553009400438484.0,
                    25234871513629598.0,
                ],
                "quote_sell_token_price": [
                    410359364.5808475,
                    1.0,
                    408619711.8704729,
                    0.538163462286757,
                    409145130.1194029,
                    1.0,
                    1.0,
                ],
                "partner_fee": [
                    "0",
                    "0",
                    "0",
                    "0",
                    "0",
                    "0",
                    "0",
                ],
                "partner_fee_recipient": [
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ],
                "protocol_fee_kind": [
                    None,
                    None,
                    "surplus",
                    None,
                    None,
                    None,
                    None,
                ],
            }
        )

        self.assertIsNone(pd.testing.assert_frame_equal(expected, rewards_df))

    def test_get_batch_rewards(self):
        block_number = 20936269
        block_range = BlockRange(block_number, block_number + 11)
        rewards_df = OrderbookFetcher.get_batch_rewards(block_range)
        expected = pd.DataFrame(
            {
                "block_number": pd.Series(
                    [20936268, 20936270, pd.NA, 20936274, pd.NA, 20936278],
                    dtype="Int64",
                ),
                "block_deadline": [
                    20936270,
                    20936272,
                    20936274,
                    20936276,
                    20936278,
                    20936280,
                ],
                "tx_hash": [
                    "0x623a8b23d2eca21f2661636b814725ac002f11b1174dbe38bdd5d2fb0e91369d",
                    "0xd099e924132c2c3be2ded1f9dae0f0910025b97f5b639260cafb8775038c60dc",
                    None,
                    "0x147119b2fcc44b8f8a34e733edf0c5fbc182e70845da1eb408f6d5b7f0722006",
                    None,
                    "0x6f133a1790397020aad2ac2e18496448c08c2a0bea2ae6e3667f3e7e2b67ffd0",
                ],
                "solver": [
                    "0x9b7e7f21d98f21c0354035798c40e9040e25787f",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x008300082c3000009e63680088f8c7f4d3ff2e87",
                    "0x0ddcb0769a3591230caa80f85469240b71442089",
                    "0x008300082c3000009e63680088f8c7f4d3ff2e87",
                    "0xc7899ff6a3ac2ff59261bd960a8c880df06e1041",
                ],
                "execution_cost": [
                    "12983200929406728",
                    "15957284641911096",
                    "0",
                    "8042014217798700",
                    "0",
                    "10406353405972980",
                ],
                "surplus": [
                    "84730642639483901",
                    "447938889765280735",
                    "0",
                    "1962451333659395",
                    "0",
                    "114169776361571642",
                ],
                "protocol_fee": [
                    "0",
                    "71690869135785700",
                    "0",
                    "0",
                    "0",
                    "26385186071416400",
                ],
                "network_fee": [
                    "9477057450023950",
                    "16474626289268700",
                    "0",
                    "7125533240475470",
                    "0",
                    "7169186891203840",
                ],
                "uncapped_payment_eth": [
                    "4049209356932509",
                    "21185897680263397",
                    "-370478108035088499",
                    "519803564128039",
                    "-357155743729567764",
                    "2367258180235328",
                ],
                "capped_payment": [
                    "4049209356932509",
                    "12000000000000000",
                    "-10000000000000000",
                    "519803564128039",
                    "-10000000000000000",
                    "2367258180235328",
                ],
                "winning_score": [
                    "84730642639483901",
                    "519629758901066469",
                    "387260436996575552",
                    "1962451333659395",
                    "374093887861962368",
                    "140554962432988068",
                ],
                "reference_score": [
                    "80681433282551392",
                    "498443861220803072",
                    "370478108035088499",
                    "1442647769531356",
                    "357155743729567764",
                    "138187704252752740",
                ],
            },
        )
        self.assertIsNone(pd.testing.assert_frame_equal(expected, rewards_df))

    def test_get_order_rewards_with_integrator_fee(self):
        block_range = BlockRange(19581572, 19581581)
        rewards_df = OrderbookFetcher.get_order_rewards(block_range)
        expected = pd.DataFrame(
            {
                "block_number": [
                    19581573,
                    19581573,
                    19581579,
                ],
                "order_uid": [
                    "0x0f6c83ff144aabed918417f61a92672165bba9b1c90f078fedfc10c2c16d03d09fa3c00a92ec5f96b1ad2527ab41b3932efeda58660e7959",
                    "0xac38b37c7821b1c0f0fd631912d7b19581c305c4bdca84dcb6d862da6cf8591cf9d7adfb5bc41283d67db9d71add65162e242f62660e7c82",
                    "0xc9c870decab00c1335babef5a3009186671b1bc69131c7338a4ecb2ed14831c321a01f63b263d93e124471e456578024121a6b78660e7a5b",
                ],
                "solver": [
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0xc74b656bd2ebe313d26d1ac02bcf95b137d1c857",
                ],
                "quote_solver": [
                    "0x16c473448e770ff647c69cbe19e28528877fba1b",
                    "0x16c473448e770ff647c69cbe19e28528877fba1b",
                    "0x16c473448e770ff647c69cbe19e28528877fba1b",
                ],
                "tx_hash": [
                    "0x13315e833ed3204db3320e3e8d213c84ab21e55e715847514d78198af4f68861",
                    "0x13315e833ed3204db3320e3e8d213c84ab21e55e715847514d78198af4f68861",
                    "0x962af6dbd9940249b8a795b48c0c4cbc04c0444555bb5b16fd4d824261e282bf",
                ],
                "surplus_fee": [
                    "958869097252287",
                    "29812970679943659325",
                    "31963685928336242096",
                ],
                "amount": [0.0, 0.0, 0.0],
                "protocol_fee": ["346011", "0", "0"],
                "protocol_fee_token": [
                    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                ],
                "protocol_fee_native_price": [
                    299379646.7519555,
                    299379646.7519555,
                    299379646.7519555,
                ],
                "quote_sell_amount": [
                    "11177061073424246",
                    "30000000000000000000000",
                    "667000000000000000000",
                ],
                "quote_buy_amount": ["37147277", "22954359609", "358516953"],
                "quote_gas_cost": [
                    3605211063076992.0,
                    1982659477433822.0,
                    2865645675046151.0,
                ],
                "quote_sell_token_price": [
                    1.0,
                    0.000234609143374563,
                    0.000160802298220274,
                ],
                "partner_fee": [
                    "346011",
                    "0",
                    "0",
                ],
                "partner_fee_recipient": [
                    "0x9FA3c00a92Ec5f96B1Ad2527ab41B3932EFEDa58",
                    None,
                    None,
                ],
                "protocol_fee_kind": [
                    "volume",
                    "priceimprovement",
                    "priceimprovement",
                ],
            }
        )

        self.assertIsNone(pd.testing.assert_frame_equal(expected, rewards_df))


if __name__ == "__main__":
    unittest.main()
