import unittest
from unittest import IsolatedAsyncioTestCase

from src.fetch.ipfs import Cid


class TestIPFS(unittest.TestCase):
    def test_cid_parsing(self):
        self.assertEqual(
            "bafybeib5q5w6r7gxbfutjhes24y65mcif7ugm7hmub2vsk4hqueb2yylti",
            str(
                Cid(
                    "0x3d876de8fcd70969349c92d731eeb0482fe8667ceca075592b8785081d630b9a"
                )
            ),
        )
        self.assertEqual(
            "bafybeia747cvkwz7tqkp67da3ehrl4nfwena3jnr5cvainmcugzocbmnbq",
            str(
                Cid(
                    "0x1FE7C5555B3F9C14FF7C60D90F15F1A5B11A0DA5B1E8AA043582A1B2E1058D0C"
                )
            ),
        )

    def test_cid_constructor(self):
        # works with or without 0x prefix:
        hex_str = "0x3d876de8fcd70969349c92d731eeb0482fe8667ceca075592b8785081d630b9a"
        self.assertEqual(Cid(hex_str), Cid(hex_str[2:]))
        self.assertEqual(hex_str, Cid(hex_str).hex)

    def test_no_content(self):
        null_cid = Cid(
            "0000000000000000000000000000000000000000000000000000000000000000"
        )

        self.assertEqual(None, null_cid.get_content(max_retries=10))

    def test_get_content(self):
        self.assertEqual(
            {
                "version": "0.1.0",
                "appCode": "CowSwap",
                "metadata": {
                    "referrer": {
                        "version": "0.1.0",
                        "address": "0x424a46612794dbb8000194937834250Dc723fFa5",
                    }
                },
            },
            Cid(
                "3d876de8fcd70969349c92d731eeb0482fe8667ceca075592b8785081d630b9a"
            ).get_content(max_retries=10),
        )

        self.assertEqual(
            {
                "version": "1.0.0",
                "appCode": "CowSwap",
                "metadata": {
                    "referrer": {
                        "kind": "referrer",
                        "referrer": "0x8c35B7eE520277D14af5F6098835A584C337311b",
                        "version": "1.0.0",
                    }
                },
            },
            Cid(
                "1FE7C5555B3F9C14FF7C60D90F15F1A5B11A0DA5B1E8AA043582A1B2E1058D0C"
            ).get_content(),
        )


class TestAsyncIPFS(IsolatedAsyncioTestCase):
    async def test_fetch_many(self):
        x = [
            {
                "app_hash": "0xa0029a1376a317ea4af7d64c1a15cb02c5b1f33c72645cc8612a8d302a6e2ac8",
                "first_seen_block": 13897827,
            },
            {
                "app_hash": "0xd8e0e541ebb486bfb4fbfeeaf097e56487b5b3ea7190bd1b286693ac02954ecd",
                "first_seen_block": 15428678,
            },
            {
                "app_hash": "0xd93cc4feb701b8b7c8013c33be82f05d28f04c127116eb3764d36881e5cd7d07",
                "first_seen_block": 13602894,
            },
            {
                "app_hash": "0x2c699c8c8b379a0ab6e6b1bc252dd2539b59b50056da1c62b2bcaf4f706d1e81",
                "first_seen_block": 13643476,
            },
            {
                "app_hash": "0xe4096788536b002e3d0af9e3a4ac44cbd5456cbd3a88d96f02c6b23be319fc6b",
                "first_seen_block": 15430776,
            },
            {
                "app_hash": "0xd7d476c0682b033fc4025f69dfb5967afe5ea96be872b1f6a740bbdc7dd97b25",
                "first_seen_block": 13671622,
            },
            {
                "app_hash": "0x37a52cce8b0b5f4b2047b8d34992d27755c9af4341290d38e4cf9278e3b8fcc9",
                "first_seen_block": 15104812,
            },
            {
                "app_hash": "0x6faca2b31493acf30239268b08ef6fa9a54ff093018c5c53a0847eb13ad8181f",
                "first_seen_block": 15557705,
            },
            {
                "app_hash": "0x476e33b8975dd54ada4a99d52c9ea4b3d41bd50763377f6e078c60631d8bc02a",
                "first_seen_block": 13783477,
            },
            {
                "app_hash": "0xd4f33cd977ef095b38aabb1ee7a2f6f69fabb4022601fdc29c9fc78c451f4e12",
                "first_seen_block": 13824523,
            },
            {
                "app_hash": "0x8b98eaf7082a14b3201011c23a39b23706d880c1269e76c154675230daf6af8d",
                "first_seen_block": 13709795,
            },
            {
                "app_hash": "0x5f3b188668cc36ab896f348913668550749c7b7f38304f45b9449cb5bea034b6",
                "first_seen_block": 13608189,
            },
            {
                "app_hash": "0xec3143ebecec4ac0f5471ce611f8220bd0abe5509c29216c6837be29fe573830",
                "first_seen_block": 13906769,
            },
            {
                "app_hash": "0x4c4d75807c15451613cead7de87e465fe9368708644fbad1e04bc442ee015466",
                "first_seen_block": 13916779,
            },
            {
                "app_hash": "0x8ee92d0defae33a7471eb0e4abac92c7be3d6811f256ecd5d439609086a9e353",
                "first_seen_block": 15684132,
            },
            {
                "app_hash": "0x5cf63a36847e6f6c98c8eb74d03cfbad8e234d7388637ae5ca53be9dd90eccca",
                "first_seen_block": 15296113,
            },
            {
                "app_hash": "0x517af09fe972bd26beb08cd951783b64a494ed22b87e088754156f9fbc9b993f",
                "first_seen_block": 13745623,
            },
            {
                "app_hash": "0x385802ecdb00e564d36aed7410454a64ecb3cfcb84792bad1ead79d1447ab090",
                "first_seen_block": 13967575,
            },
            {
                "app_hash": "0x809877e2e366166a0bcbce17b759b90d2d74af0d830a0441a44ea246876ccec0",
                "first_seen_block": 13938253,
            },
            {
                "app_hash": "0xe81711d4f01afa98128ec73441fbda767db1cf84b8c4c1bfb4243a5dddca1155",
                "first_seen_block": 13944768,
            },
            {
                "app_hash": "0x1ec8fdb53a2697ab15bc4228c9e4d3125c955470b618fcb1659175d0f6e6c991",
                "first_seen_block": 14624083,
            },
            {
                "app_hash": "0xa15b953113490d318baa1e35f1c9c33db3a6f400ce58539db287456b17bf3e58",
                "first_seen_block": 13886533,
            },
            {
                "app_hash": "0x039ffb0546992a50e4eeb70b2b6bb04c0a9a82dcad586b1c4f971d6c66109894",
                "first_seen_block": 13607684,
            },
            {
                "app_hash": "0xc56523f1422d97dcd77ab7471f3de724ac223d5a51c3af832c98a25e8ad30ab3",
                "first_seen_block": 13664176,
            },
            {
                "app_hash": "0x690483cfb64d0a175e4cb85c313e8da3e1826966acefc6697026176adacc9b19",
                "first_seen_block": 14699411,
            },
            {
                "app_hash": "0xe86fbb850981b11ab388e077c9a5a6da6cfe386c55f78709bc6549e5b806fc08",
                "first_seen_block": 13905438,
            },
            {
                "app_hash": "0xba00f0857b72309aba6a395694d999e573d6b183cf5a605341b0f3ddfba333de",
                "first_seen_block": 13779206,
            },
            {
                "app_hash": "0xdf93796ffbd982e40c53ea10517cc90eb1355eb3f843f0dafa441165a337557f",
                "first_seen_block": 13616326,
            },
            {
                "app_hash": "0x12f79d7c232ee2ef20b7aaefaf677a69d00d179d003cf5257d557d9154427f0f",
                "first_seen_block": 13971658,
            },
        ]
        results = await Cid.fetch_many(x)
        print(results)


if __name__ == "__main__":
    unittest.main()
