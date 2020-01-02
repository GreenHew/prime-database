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
    print(key_name)
    while n > total_below + total:
        bin_count += 1
        key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, bin_count)
        total_below, total, counts = s3.load_bin_from_s3('primedatabase', key_name)
    cur_n = total_below
    if cur_n < 0:
        raise Exception('Total Below metadata not set')
    lower_bound = bin_size * bin_count - bin_size
    for prime_count in counts:
        if cur_n + prime_count >= n:
            break
        lower_bound += bucket_size
        cur_n += prime_count
    bucket_primes = [p for p in stream_primes_from_n_to_m(lower_bound, upper_bound)]
    return bucket_primes[n - cur_n - 1]


if __name__ == '__main__':
    print(get_nth_prime_from_bin(int(8e7), '1e9', '1e6'))
