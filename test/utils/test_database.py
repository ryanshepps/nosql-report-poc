import unittest
from utils.database import \
    authenticate, \
    create_table, \
    delete_table, \
    bulk_load_items, \
    add_item, \
    delete_item, \
    display_table, \
    query, \
    query_rank


class Authenticate(unittest.TestCase):
    def test_should_complete_successfully(self):
        authenticate()
        self.assertTrue(True)


class CreateTable(unittest.TestCase):
    def test_should_complete_successfully(self):
        create_table()
        self.assertTrue(True)


class DeleteTable(unittest.TestCase):
    def test_should_complete_successfully(self):
        delete_table()
        self.assertTrue(True)


class BulkLoadItems(unittest.TestCase):
    def test_should_complete_successfully(self):
        bulk_load_items()
        self.assertTrue(True)


class AddItem(unittest.TestCase):
    def test_should_complete_successfully(self):
        add_item()
        self.assertTrue(True)


class DeleteItem(unittest.TestCase):
    def test_should_complete_successfully(self):
        delete_item()
        self.assertTrue(True)


class DisplayTable(unittest.TestCase):
    def test_should_complete_successfully(self):
        display_table()
        self.assertTrue(True)


class Query(unittest.TestCase):
    def test_should_complete_successfully(self):
        query()
        self.assertTrue(True)


class QueryRank(unittest.TestCase):
    def test_should_complete_successfully(self):
        query_rank()
        self.assertTrue(True)
