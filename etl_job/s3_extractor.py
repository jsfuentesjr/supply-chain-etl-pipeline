import boto3 
import pandas as pd
from io import StringIO
from config import AWS_CONFIG


def extract_from_s3():
    s3 = boto3.client('s3',
                  aws_access_key_id = AWS_CONFIG["access_key_id"],
                  aws_secret_access_key = AWS_CONFIG["secret_access_key"],
                  region_name = 'ap-southeast-1'
                 )

    response = s3.get_object(Bucket = AWS_CONFIG["s3_bucket"], Key = AWS_CONFIG["s3_key"])

    csv_content = response['Body'].read().decode('latin1')
    df_pandas = pd.read_csv(StringIO(csv_content))

    return df_pandas
