import boto3
import sys
from retrying import retry

S3BUCKET_NAME = 'defalut-bucket'
FOLDER_NAME = 'default-folder'
DATABASE_NAME = 'default'

athena = boto3.client('athena')


@retry(stop_max_attempt_number=10,
       wait_exponential_multiplier=30 * 1000,
       wait_exponential_max=10 * 60 * 1000)
def poll_status(_id):
    """
    poll query status
    """
    result = athena.get_query_execution(
        QueryExecutionId=_id
    )
    print(result['QueryExecution'])
    state = result['QueryExecution']['Status']['State']
    if state == 'SUCCEEDED':
        return result
    elif state == 'FAILED':
        return result
    else:
        raise Exception


def query_to_athena(filename):
    with open(filename, 'r') as f:
        result = athena.start_query_execution(
            QueryString=f.read(),
            QueryExecutionContext={
                'Database': DATABASE_NAME
            },
            ResultConfiguration={
                # 'OutputLocation': 's3://' + S3BUCKET_NAME,
                'OutputLocation': 's3://{0}/{1}/'.format(S3BUCKET_NAME,FOLDER_NAME)
            }
        )
    QueryExecutionId = result['QueryExecutionId']
    result = poll_status(QueryExecutionId)
    # save response
    # with open(filename + '.log', 'w') as f:
    #    f.write(pprint.pformat(result, indent=4))
    # save query result from S3
    if result['QueryExecution']['Status']['State'] == 'SUCCEEDED':
        s3_get_results(QueryExecutionId)
        return 0
    elif result['QueryExecution']['Status']['State'] == 'FAILED':
        print('SQL FAILED')
        return -1


def s3_get_results(id):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(S3BUCKET_NAME)
    obj = bucket.Object("{folder}/{key}.csv".format(folder=FOLDER_NAME, key=id))
    response = obj.get()
    body = response['Body'].read()

    for i, v in enumerate(body.decode('utf-8').splitlines()):
        print("{0}行目:{1}".format(i, v))
        # todo: ここのprintと同様に、requestを作って送信


def main():
    return query_to_athena("sample.sql")
    # return 0


if __name__ == "__main__":
    S3BUCKET_NAME = sys.argv[1]
    FOLDER_NAME = sys.argv[2]
    sys.exit(main())
