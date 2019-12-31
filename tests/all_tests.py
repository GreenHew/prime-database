import unittest
from primedb import stream_primes


class TestPrimeGeneration(unittest.TestCase):
    def setUp(self):
        pass

    def test_primes_from_n_to_m(self):
        # Test that we get 25 primes from 0 to 100
        primes = [p for p in stream_primes.stream_primes_from_n_to_m(0, 100, 10 ** 4)]
        self.assertEqual(len(primes), 25)

        # Test that we get 2 as the 1st prime from 0 to 100
        self.assertEqual(primes[0], 2)

        # Test that we get 13 as the 7th prime from 0 to 100
        self.assertEqual(primes[6], 17)

        # Test that we get 41 as the 14th prime from 0 to 100
        self.assertEqual(primes[13], 43)

        # Test that we get 16 primes form 25 to 100
        primes = [p for p in stream_primes.stream_primes_from_n_to_m(25, 100, 10 ** 4)]
        self.assertEqual(len(primes), 16)

        # Test that we get 53 as the 7th prime from 25 to 100
        self.assertEqual(primes[6], 53)

        # Test that we get 29 as the 1st prime from 25 to 100
        self.assertEqual(primes[0], 29)

        # Test that we get 43 as the 5th prime from 25 to 100
        self.assertEqual(primes[4], 43)

        # Test that we get 97 as the 16th prime from 25 to 100
        self.assertEqual(primes[15], 97)

        # Test that we get 0 primes from 98 to 99
        primes = [p for p in stream_primes.stream_primes_from_n_to_m(98, 99, 10 ** 4)]
        self.assertEqual(len(primes), 0)

        # Test that we get 1 prime from 100 to 102
        primes = [p for p in stream_primes.stream_primes_from_n_to_m(100, 102, 10 ** 4)]
        self.assertEqual(len(primes), 1)

        # Test that we get 101 as the 1st prime from 100 to 102
        self.assertEqual(primes[0], 101)

        # Test that we get 75 primes from 10^6 to 10^6 + 1,000
        primes = [p for p in stream_primes.stream_primes_from_n_to_m(10 ** 6, 10 ** 6 + 1000, 10 ** 4)]
        self.assertEqual(len(primes), 75)

        # Test that we get 1,000,003 as the 1st prime form 10^6 to 10^6 + 1000
        self.assertEqual(primes[0], 1000003)

        # Test that we get 1,000,999 as the 75th prime form 10^6 to 10^6 + 1000
        self.assertEqual(primes[74], 1000999)

        # Test that we get 2657 primes form 70,000 to 100,000
        primes = [p for p in stream_primes.stream_primes_from_n_to_m(70000, 100000, 10 ** 4)]
        self.assertEqual(len(primes), 2657)

        # Test that we get 70,001 as the 1st prime from 70,000 to 100,000
        self.assertEqual(primes[0], 70001)

        # Test that we get 99,991 as the 2657th prime from 70,000 to 100,000
        self.assertEqual(primes[2656], 99991)


if __name__ == '__main__':
    unittest.main()
