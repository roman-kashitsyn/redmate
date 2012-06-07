import unittest
import sys
import logging
from itest import integration_tests
from redmate.test import all_tests


if __name__ == "__main__":
    tests = None
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    if "--integration" in sys.argv:
        tests = integration_tests()
    else:
        tests = all_tests()

    results = unittest.TextTestRunner().run(tests)

