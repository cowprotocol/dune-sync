import unittest

from src.utils import valid_address, utc_now


class TestUtils(unittest.TestCase):
    def test_valid_address(self):
        checksum_address = "0x473780deAF4a2Ac070BBbA936B0cdefe7F267dFc"
        lower_address = checksum_address.lower()
        upper_address = lower_address.upper()
        self.assertEqual(valid_address("abc"), False)
        self.assertEqual(valid_address(lower_address), True)
        self.assertEqual(valid_address(upper_address), True)
        self.assertEqual(valid_address(checksum_address), True)

    def test_utc_now(self):
        now = utc_now()
        self.assertEqual(now.tzname(), "UTC")


if __name__ == "__main__":
    unittest.main()
