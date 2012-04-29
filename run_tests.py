import unittest
import sys
from itest import integration_tests
from redmate.test import all_tests


if __name__ == "__main__":
    tests = None
    if "--integration" in sys.argv:
        tests = integration_tests()
    else:
        tests = all_tests()

    results = unittest.TextTestRunner().run(tests)

