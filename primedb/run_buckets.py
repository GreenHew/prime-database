from primedb.stream_primes import update_total_below_s3_metadata
from primedb.multiprocess_primes import write_bins_to_s3_parallel
import sys


def main():
    bin_size, bucket_size, start, end = sys.argv[1:]
    s3_bucket = 'primedatabase'
    start = int(float(start))
    end = int(float(end))
    print(bin_size, bucket_size, start, end)
    write_bins_to_s3_parallel(bin_size, bucket_size, start, end, s3_bucket)
    update_total_below_s3_metadata(bin_size, bucket_size, s3_bucket)


if __name__ == '__main__':
    main()
