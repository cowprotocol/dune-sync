import unittest

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

        self.assertEqual(None, null_cid.get_content())

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
            ).get_content(),
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


if __name__ == "__main__":
    unittest.main()
