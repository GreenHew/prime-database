import unittest
from primedb import gen_primes


class TestPrimeGeneration(unittest.TestCase):
    def setUp(self):
        pass

    def test_primes_from_n_to_m(self):
        # Test that we get 25 primes from 0 to 100
        primes = gen_primes.primes_from_n_to_m(0, 100)
        self.assertEqual(len(primes), 25)
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()
