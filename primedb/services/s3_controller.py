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
        # print(vars(obj))
        # obj.get()['Body'].read().decode('utf-8')
        print(obj)
        total = int(obj['Metadata']['total'])
        total_below = int(obj['Metadata'].get('total_below', -1))
        body = obj['Body'].read().decode('utf-8')
        counts = [int(s) for s in body.split(',')]
        return total_below, total, counts

if __name__ == '__main__':
    ctr = S3Controller()
    print(ctr.load_bin_from_s3('primedatabase', 'prime_counts/1e6/2.txt'))

