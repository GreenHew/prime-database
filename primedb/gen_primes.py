import numpy
from primedb.services.s3_controller import S3Controller

s3 = S3Controller()


def sieve(n):
    flags = numpy.ones(n, dtype=bool)
    flags[0] = flags[1] = False
    for i in range(2, int(n ** 0.5) + 1):
        if flags[i]:
            flags[i * i::i] = False
    return numpy.flatnonzero(flags)


def primes_from_n_to_m(n, m):
    initial_primes = sieve(int(m ** 0.5) + 1)
    flags = numpy.ones(n, dtype=bool)
    flags[0] = flags[1] = False
    for prime in initial_primes:
        start = prime - n % prime
        flags[start::prime] = False
    return numpy.flatnonzero(flags) + n


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


if __name__ == '__main__':
    # print(sieve(1000))
    # write_list_to_s3(range(10), ",", 'primedatabase', '0-10.txt')
    write_nth_primes_to_s3(10 ** 6, 1000)
