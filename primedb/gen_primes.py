import numpy
from primedb.services.s3_controller import S3Controller

s3 = S3Controller()


def primesfrom2to(n):
    """ Input n>=6, Returns a array of primes, 2 <= p < n """
    sieve = numpy.ones(n // 3 + (n % 6 == 2), dtype=numpy.bool)
    for i in range(1, int(n ** 0.5) // 3 + 1):
        if sieve[i]:
            k = 3 * i + 1 | 1
            sieve[k * k // 3::2 * k] = False
            sieve[k * (k - 2 * (i & 1) + 4) // 3::2 * k] = False
    return numpy.r_[2, 3, ((3 * numpy.nonzero(sieve)[0][1:] + 1) | 1)]


def write_primes_to_s3(max_prime, bucket_size):
    primes = primesfrom2to(max_prime)
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


def write_list_to_s3(iterable, separator, bucket, key):
    data_string = separator.join(str(i) for i in iterable)
    s3.client.put_object(Bucket=bucket, Key=key, Body=data_string.encode('utf-8'))


if __name__ == '__main__':
    # print(primesfrom2to(1000))
    # write_list_to_s3(range(10), ",", 'primedatabase', '0-10.txt')
    write_primes_to_s3(10 ** 6, 100000)