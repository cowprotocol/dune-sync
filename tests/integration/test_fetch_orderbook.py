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
                    "1920099071497969074176"
                    "110185117837140289",
                    "427611951239582233762",
                    "1465179606977149402",
                    "69200861384935994787",
                ],
                "quote_gas_cost": [
                    "28980049962721130",
                    "15426768673942524",
                    "18282786590639504",
                    "18696459349125884",
                    "18629380517476576",
                    "23553009400438484",
                    "25234871513629598",
                ],
                "quote_sell_token_price": [
                    410359364.5808475,
                    1.0,
                    408619711.8704729,
                    0.538163462286757,
                    409145130.1194029,
                    1.0,
                    1.0
                ],
            }
        )

        self.assertIsNone(pd.testing.assert_frame_equal(expected, rewards_df))

    def test_get_batch_rewards(self):
        block_number = 19184750
        block_range = BlockRange(block_number, block_number + 25)
        rewards_df = OrderbookFetcher.get_batch_rewards(block_range)
        expected = pd.DataFrame(
            {
                "block_number": pd.Series(
                    [pd.NA, 19184745, 19184748, pd.NA, 19184754, 19184763],
                    dtype="Int64",
                ),
                "block_deadline": [
                    19184752,
                    19184754,
                    19184757,
                    19184760,
                    19184763,
                    19184771,
                ],
                "tx_hash": [
                    None,
                    "0xe4d32e53b60e830866c1cfdf101c60e61401078f7abdc30d50b442d131aa7bdc",
                    "0x49a757b3672f25edf2e2df52c9a466c4676e72e3c4c30ac385e065366629c56f",
                    None,
                    "0x43699f7356df831aa04519ecd9f638303f716ab9e41c6ab1f38aacd1c9cdd3e5",
                    "0x321703689c7a2743c85ad91788b8ab00447de42591089d2052a3e9cf3be9d5a1",
                ],
                "solver": [
                    "0xc74b656bd2ebe313d26d1ac02bcf95b137d1c857",
                    "0x047a2fbe8aef590d4eb8942426a24970af301875",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x6f799f4bf6c1c56fb8d890e9e0fff2934b0de157",
                ],
                "execution_cost": [
                    "0",
                    "23960377182307338",
                    "40296202766873088",
                    "0",
                    "163520173310226150",
                    "41362753732496120",
                ],
                "surplus": [
                    "0",
                    "330605523790093503",
                    "14240247437360446",
                    "0",
                    "90713565103966099",
                    "1392789908325076129",
                ],
                "protocol_fee": [
                    "0",
                    "0",
                    "0",
                    "0",
                    "6802253860482580",
                    "0",
                ],
                "network_fee": [
                    "0",
                    "13933525536949544",
                    "24543931321337540",
                    "0",
                    "123496472802428029",
                    "25234871513629600",
                ],
                "uncapped_payment_eth": [
                    "-779040158516046364",
                    "336110126639916826",
                    "38784178758697986",
                    "-30158667552343962",
                    "195046679035667621",
                    "49058033529596541",
                ],
                "capped_payment": [
                    "-10000000000000000",
                    "35960377182307338",
                    "38784178758697986",
                    "-10000000000000000",
                    "175520173310226150",
                    "49058033529596541",
                ],
                "winning_score": [
                    "781981474524515321",
                    "322695871042736445",
                    "2619667276946392",
                    "46231730550348480",
                    "50580401693171328",
                    "1373632036694856921",
                ],
                "reference_score": [
                    "779040158516046364",
                    "8428922687126221",
                    "0",
                    "30158667552343962",
                    "25965612731209087",
                    "1368966746309109188",
                ],
                "participating_solvers": [
                    [
                        "0xc74b656bd2ebe313d26d1ac02bcf95b137d1c857",
                        "0xdd2e786980cd58acc5f64807b354c981f4094936",
                    ],
                    [
                        "0x047a2fbe8aef590d4eb8942426a24970af301875",
                        "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                        "0xc74b656bd2ebe313d26d1ac02bcf95b137d1c857",
                    ],
                    ["0x4339889fd9dfca20a423fba011e9dff1c856caeb"],
                    [
                        "0x047a2fbe8aef590d4eb8942426a24970af301875",
                        "0x0ddcb0769a3591230caa80f85469240b71442089",
                        "0x16c473448e770ff647c69cbe19e28528877fba1b",
                        "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                        "0x6f799f4bf6c1c56fb8d890e9e0fff2934b0de157",
                        "0xbf54079c9bc879ae4dd6bc79bce11d3988fd9c2b",
                        "0xc74b656bd2ebe313d26d1ac02bcf95b137d1c857",
                    ],
                    [
                        "0x047a2fbe8aef590d4eb8942426a24970af301875",
                        "0x16c473448e770ff647c69cbe19e28528877fba1b",
                        "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                        "0x6f799f4bf6c1c56fb8d890e9e0fff2934b0de157",
                        "0xbf54079c9bc879ae4dd6bc79bce11d3988fd9c2b",
                        "0xc74b656bd2ebe313d26d1ac02bcf95b137d1c857",
                        "0xc9f2e6ea1637e499406986ac50ddc92401ce1f58",
                    ],
                    [
                        "0x047a2fbe8aef590d4eb8942426a24970af301875",
                        "0x0ddcb0769a3591230caa80f85469240b71442089",
                        "0x16c473448e770ff647c69cbe19e28528877fba1b",
                        "0x6f799f4bf6c1c56fb8d890e9e0fff2934b0de157",
                        "0xc74b656bd2ebe313d26d1ac02bcf95b137d1c857",
                        "0xd50ecb485dcf5d97266122dfed979587dd8923ac",
                    ],
                ],
            },
        )
        self.assertIsNone(pd.testing.assert_frame_equal(expected, rewards_df))


if __name__ == "__main__":
    unittest.main()
