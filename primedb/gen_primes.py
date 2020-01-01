import numpy
import math
from primedb.services.s3_controller import S3Controller

s3 = S3Controller()


def sieve(n):
    flags = numpy.ones(n, dtype=bool)
    flags[0] = flags[1] = False
    for i in range(2, int(n ** 0.5) + 1):
        if flags[i]:
            flags[i * i::i] = False
    return numpy.flatnonzero(flags)


def primes_from_n_to_m(n, m, initial_primes=None):
    if n < 2:
        return sieve(m)

    # We only need primes up to the root of our max number
    root_max = int(m ** 0.5)
    if initial_primes is None:
        initial_primes = sieve(root_max + 1)
    elif len(initial_primes) < root_max:
        raise Exception("initial_primes list not long enough, {} of {}".format(len(initial_primes), root_max + 1))

    flags = numpy.ones(m - n, dtype=bool)

    for prime in initial_primes:
        if prime > root_max:
            break

        # Get the first index for a number that is divisible by the prime
        start = n % prime
        if start > 0:
            start = prime - n % prime

        # We don't want to mark the prime itself as False, just multiples
        if start + n <= prime:
            start = start + prime

        flags[start::prime] = False

    return numpy.flatnonzero(flags) + n


def nth_prime_upper_bound(n):
    # https://stackoverflow.com/a/25440642/2233321
    fn = n
    primes_small = [0, 2, 3, 5, 7, 11]
    if n < 6:
        return primes_small[n]
    flog_n = math.log(n)
    flog2n = math.log(flog_n)
    if n >= 688383:
        upper = fn * (flog_n + flog2n - 1.0 + ((flog2n - 2.00) / flog_n))
    elif n >= 178974:
        upper = fn * (flog_n + flog2n - 1.0 + ((flog2n - 1.95) / flog_n))
    elif n >= 39017:
        upper = fn * (flog_n + flog2n - 0.9484)
    else:
        upper = fn * (flog_n + 0.6000 * flog2n)
    return int(math.ceil(upper))


def write_primes_to_s3(max_prime, bucket_size):
    primes = sieve(max_prime)
    cur_bucket = bucket_size
    cur_primes = []

    for prime in primes:
        if prime < cur_bucket:
            cur_primes.append(prime)
        else:
            key_name = 'primes/{}-{}.txt'.format(cur_bucket - bucket_size, cur_bucket)
            write_list_to_s3(cur_primes, ',', 'primedatabase', key_name)
            cur_primes = []
            cur_bucket += bucket_size


def write_nth_primes_to_s3(max_prime, bucket_size):
    primes = sieve(max_prime)
    cur_bucket = bucket_size
    cur_primes = []

    for prime in primes:
        if len(cur_primes) < bucket_size - 1:
            cur_primes.append(prime)
        else:
            key_name = 'nth_primes/{}/{}-{}.txt'.format(bucket_size, cur_bucket - bucket_size + 1, cur_bucket)
            write_list_to_s3(cur_primes, ',', 'primedatabase', key_name)
            cur_primes = []
            cur_bucket += bucket_size


def write_list_to_s3(iterable, separator, bucket, key):
    data_string = separator.join(str(i) for i in iterable)
    s3.client.put_object(Bucket=bucket, Key=key, Body=data_string.encode('utf-8'))


def generate_prime_bucket_counts(bin_size, bucket_size, start, end):
    initial_primes = sieve(int(end ** 0.5) + 1)
    n = start
    m = start + bucket_size
    prime_counts = []

    while m < end:
        bucket_primes = primes_from_n_to_m(n, m, initial_primes)
        count = len(bucket_primes)
        prime_counts.append(count)
        n = m + 1
        m = m + bucket_size


if __name__ == '__main__':
    # print(sieve(1000))
    # write_list_to_s3(range(10), ",", 'primedatabase', '0-10.txt')
    # write_nth_primes_to_s3(10 ** 6, 1000)
    print(len(sieve(int(1e6))))
