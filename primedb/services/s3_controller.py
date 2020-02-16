import boto3


class S3Controller:
    def __init__(self):
        self.client = boto3.client("s3")

    def write_list_to_s3(self, iterable, bucket, key, separator=','):
        data_string = separator.join(str(i) for i in iterable)
        self.client.put_object(Bucket=bucket, Key=key, Body=data_string.encode('utf-8'), Metadata={
            'total': str(sum(iterable))
        })

    def load_bin_from_s3(self, bucket, key):
        obj = self.client.get_object(Bucket=bucket, Key=key)
        total = int(obj['Metadata']['total'])
        total_below = int(obj['Metadata'].get('total_below', -1))
        body = obj['Body'].read().decode('utf-8')
        counts = [int(s) for s in body.split(',')]
        return total_below, total, counts

    def get_matching_s3_keys(self, bucket, prefix='', suffix=''):
        """
        Generate the keys in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        kwargs = {'Bucket': bucket}

        # If the prefix is a single string (not a tuple of strings), we can
        # do the filtering directly in the S3 API.
        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix

        while True:

            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = self.client.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix):
                    yield key

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def bucket_exists(self, key_name):
        try:
            self.load_bin_from_s3('primedatabase', key_name)
        except:
            return False
        return True

if __name__ == '__main__':
    ctr = S3Controller()
    print(ctr.load_bin_from_s3('primedatabase', 'prime_counts/1e6/2.txt'))

