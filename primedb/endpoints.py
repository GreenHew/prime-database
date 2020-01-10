from primedb.services.s3_controller import S3Controller
from primedb.stream_primes import stream_primes_from_n_to_m
from primedb.gen_primes import nth_prime_upper_bound

s3 = S3Controller()


def example_endpoint(body):
    return {'status': 'success'}, 200


def get_nth_prime(n, bucket_size=1000):
    prime_index = n % bucket_size - 1
    bucket_start = n - prime_index
    key_name = 'nth_primes/{}/{}-{}.txt'.format(bucket_size, bucket_start, bucket_start + bucket_size - 1)
    prime_string = s3.client.get_object(Bucket='primedatabase', Key=key_name)['Body'].read().decode('utf-8')
    prime = prime_string.split(',')[prime_index]
    return prime


def get_nth_prime_from_bin(n, bin_size_sci, bucket_size_sci):
    bucket_size = int(float(bucket_size_sci))
    bin_size = int(float(bin_size_sci))
    upper_bound = nth_prime_upper_bound(n) + 1

    bin_count = upper_bound // bin_size + 1
    key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, bin_count)

    total_below, total, counts = s3.load_bin_from_s3('primedatabase', key_name)

    while n <= total_below:
        bin_count -= 1
        key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, bin_count)
        total_below, total, counts = s3.load_bin_from_s3('primedatabase', key_name)

    print(key_name)

    cur_n = total_below

    if cur_n < 0:
        raise Exception('Total Below metadata not set')

    lower_bound = bin_size * bin_count - bin_size

    for prime_count in counts:
        if cur_n + prime_count >= n:
            break
        lower_bound += bucket_size
        cur_n += prime_count

    upper_bound = min(upper_bound, lower_bound + bucket_size)
    bucket_primes = list(stream_primes_from_n_to_m(lower_bound, upper_bound))

    return bucket_primes[n - cur_n - 1]

def get_prime_count_from_n_to_m(n, m, bin_size_sci, bucket_size_sci):
    bin_size = int(float(bin_size_sci))
    bucket_size = int(float(bucket_size_sci))
    count = 0

    # make count inclusive from n to m
    m += 1

    # small range, calculate directly
    if m - n < bucket_size * 2:
        for prime in stream_primes_from_n_to_m(n, m):
            count += 1
        return count

    cur_bin = n // bin_size + 1
    cur_bucket = (n - bin_size * (cur_bin - 1)) // bucket_size
    if n % bucket_size > 0:
        cur_bucket += 1

    # count primes upto the first bucket
    if n % bucket_size > 0:
        cur_m = cur_bucket * bucket_size
        for prime in stream_primes_from_n_to_m(n, cur_m):
            count += 1
        print(n, m, count)

    # load prime counts from s3 and count buckets
    while m > bin_size * (cur_bin - 1):
        key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, cur_bin)
        total_below, total, counts = s3.load_bin_from_s3('primedatabase', key_name)
        while m > (cur_bucket + 1) * bucket_size + bin_size * (cur_bin - 1) and cur_bucket < len(counts):
            count += counts[cur_bucket]
            cur_bucket += 1
        cur_bin += 1
        cur_bucket = 0

    # count the remaining primes
    if m % bucket_size > 0:
        cur_n = m - m % bucket_size
        for prime in stream_primes_from_n_to_m(cur_n, m, 10 ** 9):
            count += 1
    return count

def get_sequence_number_for_prime_n(n, bin_size_sci, bucket_size_sci):
    bin_size = int(float(bin_size_sci))
    bucket_size = int(float(bucket_size_sci))

    cur_bin = int(n // bin_size + 1)
    key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, cur_bin)
    total_below, total, counts = s3.load_bin_from_s3('primedatabase', key_name)

    count = total_below + 1
    buckets_to_count = int((bin_size - (cur_bin * bin_size - n)) // bucket_size)
    for i in range(buckets_to_count):
        count += counts[i]

    # count remaining primes
    start = n - n % bucket_size
    for prime in stream_primes_from_n_to_m(start, n):
        count += 1
    return count




if __name__ == '__main__':
    # 710000000
    # 712790800 and 712799800
    # 712700000
    # endish of bin 16 - 712721450
    # startish of off by one 713732500

    # print(get_nth_prime_from_bin(int(1e9+12123), '1e9', '1e6'))
    print(get_prime_count_from_n_to_m(int(3e6 + 12345), int(2e9+123132), '1e9', '1e6'))
    # print(get_sequence_number_for_prime_n(int(2), '1e9', '1e6'))

    # 23,365