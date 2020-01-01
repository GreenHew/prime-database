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


def get_nth_prime_from_bin(n, bucket_size=10 ** 7):
    upper_bound = nth_prime_upper_bound(n)
    cur_n = 0
    lower_bound = 1
    key_name = 'prime_counts/{}/{}.txt'.format(bin_size_sci, int(cur_m / bin_size))
    for prime_count in example_bin:
        if cur_n + prime_count > n:
            break
        lower_bound += bucket_size
        cur_n += prime_count
    bucket_primes = [p for p in stream_primes_from_n_to_m(lower_bound, upper_bound)]
    return bucket_primes[n - cur_n - 1]


if __name__ == '__main__':
    print(get_nth_prime(52592))
