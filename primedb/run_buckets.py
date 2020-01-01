from primedb.stream_primes import write_bins_to_s3, update_total_below_s3_metadata
import sys


def main():
    bin_size, bucket_size, start, end = sys.argv[1:]
    s3_bucket = 'primedatabase'
    start = int(float(start))
    end = int(float(end))
    write_bins_to_s3(bin_size, bucket_size, start, end, s3_bucket)
    update_total_below_s3_metadata(bin_size, bucket_size, s3_bucket)


if __name__ == '__main__':
    main()
