import time
import multiprocessing
from primedb import stream_primes
from primedb.services.s3_controller import S3Controller

s3 = S3Controller()

def generate_bucket_counts_and_write_to_s3(bucket_size, start, end, s3_bucket_name, key_name):
    start_time = time.time()

    bin = stream_primes.generate_prime_bucket_counts(bucket_size, start, end)
    s3.write_list_to_s3(bin, s3_bucket_name, key_name)
    # Log the time it is taking
    print(key_name, round(time.time() - start_time, 2), "(s)")


def write_bins_to_s3_parallel(bin_size_sci, bucket_size_sci, start, end, s3_bucket_name):
    bin_size = int(float(bin_size_sci))
    bucket_size = int(float(bucket_size_sci))

    # Don't spawn more processes than CPUs
    max_processes = multiprocessing.cpu_count() - 1
    print('Max processes: {}'.format(max_processes))

    cur_n = start
    cur_m = start + bin_size

    start_time = time.time()

    # Start a multiprocessing queue
    with multiprocessing.Pool(max_processes) as pool:
        processes = []
        while cur_m <= end:
            bin_number = int(cur_m / bin_size)

            # Print our total time every 10 bins
            if not bin_number % 10:
                print('On bin: {}, Total time: {} (m)'.format(bin_number, round((time.time() - start_time) / 60, 2)))

            key_name = 'prime_counts/{}/{}/{}.txt'.format(bin_size_sci, bucket_size_sci, bin_number)
            p = pool.apply_async(generate_bucket_counts_and_write_to_s3, (bucket_size, cur_n, cur_m, s3_bucket_name, key_name))
            processes.append(p)

            # Remove completed processes
            processes = [p for p in processes if not p.ready()]

            # If we have a lot of tasks we don't want to waste memory adding them to the queue if it is backed up
            if len(processes) > max_processes * 2:
                time.sleep(1)

            cur_n = cur_m
            cur_m = cur_n + bin_size

        pool.close()
        pool.join()

if __name__ == '__main__':
    write_bins_to_s3_parallel('1e8', '1e6', 0, 4e9, 'primedatabase')
