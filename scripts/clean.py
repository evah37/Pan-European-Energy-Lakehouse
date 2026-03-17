import boto3

MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "minio"
SECRET_KEY = "minio123"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)


def reset_lake():
    # try to delete the old raw bucket and its contents
    try:
        print("cleaning raw bucket...")
        res = s3.list_objects_v2(Bucket='energy-lake')
        if 'Contents' in res:
            for obj in res['Contents']:
                s3.delete_object(Bucket='energy-lake', Key=obj['Key'])
        s3.delete_bucket(Bucket='energy-lake')
        print("cleaned raw bucket")
    except Exception as e:
        print(f"skip cleaning raw bucket: {e}")

    # create or clean the new 'energy-lake' bucket
    try:
        s3.create_bucket(Bucket='energy-lake')
        print("created energy-lake bucket")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print("energy-lake already exists, cleaning contents...")
        res = s3.list_objects_v2(Bucket='energy-lake')
        if 'Contents' in res:
            for obj in res['Contents']:
                s3.delete_object(Bucket='energy-lake', Key=obj['Key'])
        print("energy-lake cleaned")


if __name__ == "__main__":
    reset_lake()
