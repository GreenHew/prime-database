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
    if n <= 0:
        return {'message': 'Input must be greater than 0'}, 422
    bucket_size = int(float(bucket_size_sci))
    bin_size = int(float(bin_size_sci))
    upper_bound = nth_prime_upper_bound(n) + 1

    bin_count = upper_bound // bin_size + 1
    key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, bin_count)
    try:
        total_below, total, counts = s3.load_bin_from_s3('primedatabase', key_name)
    except:
        return {'message': 'Input out of range'}, 422

    while n <= total_below:
        bin_count -= 1
        key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, bin_count)
        total_below, total, counts = s3.load_bin_from_s3('primedatabase', key_name)

    # print(key_name)

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
    print(n - cur_n - 1, len(bucket_primes), total + total_below)

    return {'nth_prime': int(bucket_primes[n - cur_n - 1])}, 200

def get_prime_count_up_to_n(n, bin_size_sci, bucket_size_sci):
    bin_size = int(float(bin_size_sci))
    bucket_size = int(float(bucket_size_sci))
    count = 0
    n += 1 # make inclusive

    # small range, calculate directly
    if n < bucket_size:
        if n > 0:
            for prime in stream_primes_from_n_to_m(1, n):
                count += 1
        return {'count': count}, 200

    cur_bin = n // bin_size
    cur_bucket = (n - bin_size * cur_bin) // bucket_size
    key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, cur_bin + 1)
    try:
        total_below, total, counts = s3.load_bin_from_s3('primedatabase', key_name)
    except:
        return {'message': 'Input out of range'}, 422

    # add prime count before bin and all buckets before n
    count = total_below
    for i in range(cur_bucket):
        count += counts[i]

    # count remaining primes up to n
    if n % bucket_size > 0:
        for _ in stream_primes_from_n_to_m(cur_bin * bin_size + cur_bucket * bucket_size, n):
            count += 1
    return {'count': count}, 200

def get_prime_count_from_n_to_m(n, m, bin_size_sci, bucket_size_sci):
    if m < n:
        return {'message': 'Invalid range'}, 422
    if n <= 2:
        return get_prime_count_up_to_n(m, bin_size_sci, bucket_size_sci)
    n_resp, n_status = get_prime_count_up_to_n(n, bin_size_sci, bucket_size_sci)
    m_resp, m_status = get_prime_count_up_to_n(m, bin_size_sci, bucket_size_sci)
    if n_status == 422 or m_status == 422:
        return {'message': 'Input out of range'}, 422
    return {'count': m_resp['count'] - n_resp['count']}, 200

def get_sequence_number_for_prime_n(n, bin_size_sci, bucket_size_sci):
    # returns sequence number if prime, -1 if not a prime, or -2 if n out of range.
    bin_size = int(float(bin_size_sci))
    bucket_size = int(float(bucket_size_sci))

    # return -1 if n not a prime
    if n % 2 == 0 or n % bin_size == 0 or n % bucket_size == 0:
        return {'is_prime': False}, 200

    is_prime = False
    count = 0

    # calculate small range
    if n < bucket_size:
        for prime in stream_primes_from_n_to_m(1, n + 1):
            count += 1
            if prime == n:
                is_prime = True
        if is_prime:
            return {'is_prime': True,
                    'sequence_number': count
                    }, 200
        else:
            return {'is_prime': False}, 200

    cur_bin = int(n // bin_size + 1)
    key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, cur_bin)
    try:
        total_below, total, counts = s3.load_bin_from_s3('primedatabase', key_name)
    except:
        return {'message': 'Input out of range'}, 422

    count = total_below
    buckets_to_count = int((bin_size - (cur_bin * bin_size - n)) // bucket_size)
    for i in range(buckets_to_count):
        count += counts[i]

    # count remaining primes
    start = n - n % bucket_size
    for prime in stream_primes_from_n_to_m(start, n + 1):
        count += 1
        if prime == n:
            is_prime = True
    if is_prime:
        return {'is_prime': True,
                'sequence_number': count
                }, 200
    else:
        return {'is_prime': False}, 200




if __name__ == '__main__':
    # 710000000
    # 712790800 and 712799800
    # 712700000
    # endish of bin 16 - 712721450
    # startish of off by one 713732500

    # print(get_nth_prime_from_bin(int(3.2*10**12), '1e9', '1e6'))
    # print(get_prime_count_from_n_to_m(12345,  100, '1e9', '1e6'))
    # print(get_prime_count_up_to_n(9*10**13, '1e9', '1e6'))
    print(get_sequence_number_for_prime_n(4983906313, '1e9', '1e6'))

    # 23,365