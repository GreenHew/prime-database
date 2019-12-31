import numpy
import time


def sieve(n):
    flags = numpy.ones(n, dtype=bool)
    flags[0] = flags[1] = False
    for i in range(2, int(n ** 0.5) + 1):
        if flags[i]:
            flags[i * i::i] = False
    return numpy.flatnonzero(flags)


def primes_from_n_to_m(n, m, prime_stream):
    if n < 2:
        return sieve(m)

    flags = numpy.ones(m - n, dtype=bool)
    root_max = int(m ** 0.5)

    for prime in prime_stream:
        if prime > root_max:
            break

        # Get the first index for a number that is divisible by the prime
        start = n % prime
        if start > 0:
            start = prime - start

        # We don't want to mark the prime itself as False, just multiples
        if start + n <= prime:
            start = start + prime

        flags[start::prime] = False

    return numpy.flatnonzero(flags) + n


def stream_primes_from_n_to_m(n, m, batch_size):
    if m - n <= batch_size:
        m_root = int(m ** 0.5)

        # If the number primes needed to generate this batch are too large, recursively stream them too
        if m_root < batch_size:
            sub_primes = sieve(m)
        else:
            sub_primes = stream_primes_from_n_to_m(0, m_root, batch_size)

        # This is our non streaming base case
        for prime in primes_from_n_to_m(n, m, sub_primes):
            yield prime
    else:
        # The number of primes needed is larger than our batch size
        cur_n = n
        while cur_n < m:
            cur_m = cur_n + batch_size
            # Recursively stream primes in smaller batch sizes
            for prime in stream_primes_from_n_to_m(cur_n, cur_m, batch_size):
                yield prime
            cur_n += batch_size


def print_occasional_primes():
    start_time = time.time()
    last_time = start_time
    i = 0
    batch_size = 10 ** 7
    print_size = 10 ** 7
    print('batch size {}'.format(batch_size))
    for prime in stream_primes_from_n_to_m(0, 10 ** 18, batch_size):
        i += 1
        if i > print_size:
            new_time = time.time()
            print(prime, "{}s ({} total)".format(new_time - last_time, new_time - start_time))
            i = 0
            last_time = new_time


def generate_prime_bucket_counts(bucket_size, start, end):
    n = start
    m = start + bucket_size - 1
    prime_counts = []

    while m < end:
        count = 0
        for prime in stream_primes_from_n_to_m(n, m, 10 ** 7):
            count += 1
        print(n, m, count)
        prime_counts.append(count)
        n = m + 1
        m = m + bucket_size
    return prime_counts


if __name__ == '__main__':
    # print_occasional_primes()
    counts = generate_prime_bucket_counts(10 ** 7, 10 ** 0, 10 ** 10 + 10 ** 8)
    print(counts)
