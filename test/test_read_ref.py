import unittest
import reference


class TestReadReference(unittest.TestCase):
    def test_found(self):
        cur = reference.open_reference_database("./test.db")
        old_value = "0000001";

        result = reference.read_reference_entry(cursor=cur, reference_name="testref", old_value=old_value)
        self.assertEqual(10000001, result)
        reference.close_database_connection(cursor=cur)

    def test_notfound(self):
        cur = reference.open_reference_database("./test.db")
        old_value = "0200001";

        result = reference.read_reference_entry(cursor=cur, reference_name="testref", old_value=old_value)
        self.assertEqual(None, result)
        reference.close_database_connection(cursor=cur)


if __name__ == '__main__':
    unittest.main()
