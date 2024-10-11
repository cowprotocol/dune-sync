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
        block_number = 20934809
        block_range = BlockRange(block_number, block_number + 17)
        rewards_df = OrderbookFetcher.get_order_rewards(block_range)
        expected = pd.DataFrame(
            {
                "block_number": [
                    20934810,
                    20934812,
                    20934816,
                    20934819,
                    20934821,
                    20934823,
                    20934823,
                ],
                "order_uid": [
                    "0xc129dbf2f44b67a570a161860de24074a739c6d95d23eee736149f6ea55d84ac36eb8d32526284fa6f2dd151b982e52988d7b5f86707bc6b",
                    "0x8eb73ae608594d507a5cfb0d90088d6cf2ec431dca46505a460fbd3dbaf07acf04fbb30283c28fa7e989ed5c91a2fc338973f34e6707b7fe",
                    "0x1579de80933bfe2397a685d246e309ea4a9b8696597b8b957cabe0921f3eb8b9dc1e9a21b7ed8dfb87c997e7d0405e2cfcfa89386707bcca",
                    "0xe9464aea08101cd1c5ecb6e896554b502e7e9759e9f7159189311790aae698c28a1f2c41f6f96730623d860c45ec01a8a63b3ff86707bc95",
                    "0xf933af0bcf500b5499d9802f01f85dd13d0cde0e48aa08cefadb738438a2b06540a50cf069e992aa4536211b23f286ef88752187ffffffff",
                    "0xe04adff20d737ca106ba70856bf7e0cc1800d5de1783a5dbc49c555e3fc6463940a50cf069e992aa4536211b23f286ef88752187ffffffff",
                    "0xb0b69286396dd53acb74e9fb25d6a9a72dd81189e3d301140c9f0cb5f2fb988b3e4aea5c2e1433910621b4378dfa541264fc145b6707bcfb",
                ],
                "solver": [
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0xc7899ff6a3ac2ff59261bd960a8c880df06e1041",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                ],
                "quote_solver": [
                    "0x008300082c3000009e63680088f8c7f4d3ff2e87",
                    "0x008300082c3000009e63680088f8c7f4d3ff2e87",
                    "0xc74b656bd2ebe313d26d1ac02bcf95b137d1c857",
                    "0xc10d4dfda62227d9ec23ab0010e2942e48338a60",
                    "0x4339889fd9dfca20a423fba011e9dff1c856caeb",
                    "0xc7899ff6a3ac2ff59261bd960a8c880df06e1041",
                    "0x008300082c3000009e63680088f8c7f4d3ff2e87",
                ],
                "tx_hash": [
                    "0x54cd9572979c952f2e1b0a966e42ad004e243367f6349e2ce56d80e91db2978a",
                    "0x425b0d49dfdf860c08d21da224f20a2bb7606c8030ddb5b21b4cd98a70502740",
                    "0x84cf5e44ce00d82105702d7f45e20a9fb60a479e494262ccf621b79f7770a490",
                    "0x3483aaa340e524077f857d3d94e05a551839cc835909ac81450e21f22c902a79",
                    "0x96318915e712ea346fa99cbe58519db6767bc45aad6e1fcc92f48f5a02b8e034",
                    "0xba7475c759c289e85ee0dd3319e39e405d26e3066f457df50c9110a64e601e6b",
                    "0xba7475c759c289e85ee0dd3319e39e405d26e3066f457df50c9110a64e601e6b",
                ],
                "surplus_fee": [
                    "2266472706677075",
                    "1189015100233555732594687",
                    "2837993604",
                    "5512297385347536",
                    "2302819091231050",
                    "4797029976282923",
                    "407690373176599",
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
                    "467895383394304",
                    "2286532007592200000",
                    "17883214118912000",
                    "0",
                    "297494968688870",
                ],
                "protocol_fee_token": [
                    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                    "0xf19308f923582a6f7c465e5ce7a9dc1bec6665b1",
                    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                    "0xec53bf9167f50cdeb3ae105f56099aaab9061f83",
                    "0x3106a0a076bedae847652f42ef07fd58589e001f",
                    "0xfd0205066521550d7d7ab19da8f72bb004b4c341",
                    "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                ],
                "protocol_fee_native_price": [
                    1.0,
                    4.14627277e-10,
                    1.0,
                    0.001495472574158779,
                    0.00008227599141422,
                    0.000012405155005943,
                    1.0,
                ],
                "quote_sell_amount": [
                    "109899794590184336",
                    "6678762644284597849428691687",
                    "8000000000000",
                    "5073337115967525579",
                    "1469072907309463969",
                    "6000000000000000000",
                    "29670143598685664",
                ],
                "quote_buy_amount": [
                    "110611848308261632",
                    "22567439059361636779445714944",
                    "8849796343220294600",
                    "3384931672358364844032",
                    "17598530665993045803008",
                    "476716533757541695910447",
                    "29862362965928048",
                ],
                "quote_gas_cost": [
                    2195335897623140.2,
                    1497753531187998.5,
                    3278739441054120.0,
                    2398711536964080.0,
                    2053290592851789.0,
                    3228942613973600.0,
                    2097897905068290.0,
                ],
                "quote_sell_token_price": [
                    1.0073075605014072,
                    1.3148021574880433e-9,
                    1111228.9670799475,
                    1,
                    1,
                    1,
                    1.0073055030210916,
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
                    "priceimprovement",
                    "priceimprovement",
                    "priceimprovement",
                    "priceimprovement",
                    "priceimprovement",
                    "priceimprovement",
                    "priceimprovement",
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
        block_range = BlockRange(20934805, 20934809)
        rewards_df = OrderbookFetcher.get_order_rewards(block_range)
        expected = pd.DataFrame(
            {
                "block_number": [
                    20934806,
                    20934808,
                ],
                "order_uid": [
                    "0x3d388aacd736a1043ca403b0ecfd9cd4fd1a46cebab99f74637d7d1bfd08200631d7916593d1ed6cdb3b1ff2a1f656a2eb2a23816707bbfd",
                    "0xad7064d19ab9398fc4a0f17e57bea8d4ed83a71d91c1798e3da01199540b970eecf1c70100107017c5da1ff4bbcc482809e396ee6707ba81",
                ],
                "solver": [
                    "0x755bae1cd46c9c27a3230aef0ce923bda13d29f7",
                    "0xd1508a211d98bb81195dc1f9533edcdf68adf036",
                ],
                "quote_solver": [
                    "0x4fc4a61a3b99a1ad4a61b03f3752ca12b4a17646",
                    "0xd1508a211d98bb81195dc1f9533edcdf68adf036",
                ],
                "tx_hash": [
                    "0x08f51e088100f4afddf4012e7b6b26d292861ceda92723fd764fff3e1af49c0d",
                    "0x6ea58eb10d57134c0fb0701c208292eade107870029d0653404a2371d63588fa",
                ],
                "surplus_fee": [
                    "7606",
                    "144778705",
                ],
                "amount": [0.0, 0.0],
                "protocol_fee": ["1329681", "140147111"],
                "protocol_fee_token": [
                    "0xdac17f958d2ee523a2206206994597c13d831ec7",
                    "0xdac17f958d2ee523a2206206994597c13d831ec7",
                ],
                "protocol_fee_native_price": [
                    417562067.399692,
                    417562067.399692,
                ],
                "quote_sell_amount": [
                    "2500000",
                    "200000000000",
                ],
                "quote_buy_amount": ["1523804298", "200214933429"],
                "quote_gas_cost": [
                    1808592978505935,
                    1516678038740264.2,
                ],
                "quote_sell_token_price": [
                    254238788483.4054,
                    417824591.695399,
                ],
                "partner_fee": [
                    "0",
                    "140147111",
                ],
                "partner_fee_recipient": [
                    None,
                    "0x63695Eee2c3141BDE314C5a6f89B98E62808d716",
                ],
                "protocol_fee_kind": [
                    "priceimprovement",
                    "priceimprovement",
                ],
            }
        )

        self.assertIsNone(pd.testing.assert_frame_equal(expected, rewards_df))


if __name__ == "__main__":
    unittest.main()
