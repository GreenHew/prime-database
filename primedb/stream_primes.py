import numpy
import time
from primedb.services.s3_controller import S3Controller

s3 = S3Controller()


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


def stream_primes_from_n_to_m(n, m, batch_size=10 ** 7):
    if m - n <= batch_size:
        m_root = int(m ** 0.5)

        # If the number primes needed to generate this batch are too large, recursively stream them too
        if m_root < batch_size:
            sub_primes = sieve(m_root)
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
    cur_bucket_end = start + bucket_size
    prime_counts = []
    count = 0
    for prime in stream_primes_from_n_to_m(start, end, 10 ** 9):
        if prime > cur_bucket_end:
            prime_counts.append(count)
            count = 0
            cur_bucket_end += bucket_size
        count += 1
    prime_counts.append(count)
    return prime_counts


def generate_prime_bucket_bins(bin_size, bucket_size, start, end):
    cur_n = start
    cur_m = start + bin_size
    while cur_m <= end:
        yield cur_n, cur_m, generate_prime_bucket_counts(bucket_size, cur_n, cur_m)
        cur_n = cur_m
        cur_m = cur_n + bin_size


def write_bins_to_s3(bin_size_sci, bucket_size_sci, start, end, s3_bucket_name):
    bin_size = int(float(bin_size_sci))
    bucket_size = int(float(bucket_size_sci))
    bins = generate_prime_bucket_bins(bin_size, bucket_size, start, end)

    start_time = time.time()
    cur_time = start_time

    for cur_n, cur_m, bin in bins:
        key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, int(cur_m / bin_size))

        # Log the time it is taking
        new_time = time.time()
        print(key_name, round(new_time - cur_time, 2), "(s),", round((new_time - start_time) / 60, 2), "(m) total")

        cur_time = new_time
        s3.write_list_to_s3(bin, s3_bucket_name, key_name)


def update_total_below_s3_metadata(bin_size_sci, bucket_size_sci, s3_bucket_name):
    prefix = 'prime_counts/{}/{}'.format(bin_size_sci, bucket_size_sci)
    keys = [k for k in s3.get_matching_s3_keys(s3_bucket_name, prefix)]
    keys.sort(key=lambda k: int(k.split('/')[-1].split('.')[0]))
    sum_total_below = 0
    for key in keys:
        obj = s3.client.get_object(Bucket=s3_bucket_name, Key=key)
        total = obj['Metadata']['total']
        total_below = obj['Metadata'].get('total_below')
        print(sum_total_below, key)
        if total_below is None:
            kwargs = {'Bucket': s3_bucket_name, 'Key': key}
            s3.client.copy_object(**kwargs, Metadata={
                'total': total,
                'total_below': str(sum_total_below)
            }, CopySource=kwargs, MetadataDirective='REPLACE')
        sum_total_below += int(total)


if __name__ == '__main__':
    # print_occasional_primes()
    # counts = generate_prime_bucket_counts(10 ** 7, 10 ** 0, 10 ** 10 + 10 ** 8)
    # print(counts)
    # print(len(list(stream_primes_from_n_to_m(0, int(60000), 10 ** 9))))
    write_bins_to_s3('1e9', '1e6', 0, 1e12, 'primedatabase')
    update_total_below_s3_metadata('1e9', '1e6', 'primedatabase')
