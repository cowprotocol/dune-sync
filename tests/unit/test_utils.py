import unittest

from src.fetch.evm.events import address_from_topic, integer_from_hex_data


class MyTestCase(unittest.TestCase):
    def test_address_from_topic(self):
        self.assertEqual(
            address_from_topic(
                "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41"
            ),
            "0x9008d19f58aabd9ed0d60971565aa8510560ab41",
        )

        with self.assertRaises(AssertionError):
            # not a valid topic containing address.
            address_from_topic(
                "0x1000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41"
            )

    def test_integer_from_hex_data(self):
        self.assertEqual(
            integer_from_hex_data(
                "0x0000000000000000000000009008d19f58aabd9ed0d60971565aa8510560ab41"
            ),
            822291337650735814848589760912749869912345848641,
        )
        # Largest Possible Number!
        self.assertEqual(integer_from_hex_data("0x" + "f" * 64), pow(2, 256) - 1)
        # Small Numbers
        self.assertEqual(
            integer_from_hex_data(
                "0x0000000000000000000000000000000000000000000000000000000000000000"
            ),
            0,
        )

        self.assertEqual(
            integer_from_hex_data(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            ),
            1,
        )
        self.assertEqual(
            integer_from_hex_data(
                "0x000000000000000000000000000000000000000000000000000000000000000f"
            ),
            15,
        )


if __name__ == "__main__":
    unittest.main()
