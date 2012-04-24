import unittest
from test import all_tests


if __name__ == "__main__":
    tests = all_tests()
    results = unittest.TextTestRunner().run(tests)

