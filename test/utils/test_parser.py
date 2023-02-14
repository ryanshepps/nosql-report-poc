import unittest
from modules.parser import \
    csv_to_json


class CSVToJSON(unittest.TestCase):
    def test_should_complete_successfully(self):
        csv_to_json()
        self.assertTrue(True)
