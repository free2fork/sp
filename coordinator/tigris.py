import boto3
import os

BUCKET_NAME = os.environ.get("BUCKET_NAME", "duckpond-data")
TIGRIS_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL_S3", "https://fly.storage.tigris.dev")

_s3_client = None

def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client(
            's3',
            endpoint_url=TIGRIS_ENDPOINT,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=boto3.session.Config(s3={'addressing_style': 'virtual'}, signature_version='s3v4')
        )
    return _s3_client

def bucket_exists():
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        return True
    except:
        return False

_url_cache = {}

def list_table_files(prefix="data/", presign: bool = False):
    global _url_cache
    import time
    
    # Preserve DuckDB HTTP metadata cache by forcing identical S3 URL signatures per half-hour
    cache_key = f"{prefix}_{presign}"
    if cache_key in _url_cache:
        cached_data, timestamp = _url_cache[cache_key]
        if time.time() - timestamp < 2700: # 45 minutes
            return cached_data

    client = get_s3_client()
    res = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    
    files = []
    ext_type = ".parquet" # default analyzer mode
    
    if 'Contents' in res:
        for obj in res['Contents']:
            key = obj['Key']
            if key.endswith(('.parquet', '.csv', '.json', '.avro')):
                # deduce format based on physical footprint
                if key.endswith('.csv'): ext_type = '.csv'
                elif key.endswith('.json'): ext_type = '.json'
                elif key.endswith('.avro'): ext_type = '.avro'
                else: ext_type = '.parquet'
                
                if presign:
                    url = client.generate_presigned_url('get_object', Params={'Bucket': BUCKET_NAME, 'Key': key}, ExpiresIn=3600)
                    files.append(url)
                else:
                    files.append(f"s3://{BUCKET_NAME}/{key}")
                    
    payload = (files, ext_type)
    _url_cache[cache_key] = (payload, time.time())
    return payload

def get_directory_size(prefix: str):
    client = get_s3_client()
    try:
        res = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        total_size = sum(obj['Size'] for obj in res.get('Contents', []))
        total_files = len(res.get('Contents', []))
        return total_size, total_files
    except:
        return 0, 0
