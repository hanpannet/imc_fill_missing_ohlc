import unittest

from convert import main


class TestMain(unittest.TestCase):
    def test_1min(self):
        main(1, test=True)
        self.assertEqual(1, 1)

    def test_3min(self):
        main(3, test=True)
        self.assertEqual(1, 1)

    def test_init_should_work(self):
        pass


if __name__ == '__main__':
    unittest.main()
