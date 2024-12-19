from config.config import S3_BUCKET
from config.config import AWS_ACCESS_KEY
from config.config import AWS_SECRET_KEY
from config.config import AWS_REGION
from config.config import DATALAKE_TYPE
from include.datalake.local_datalake import LocalDatalake
from include.datalake.s3_datalake import S3Datalake

def get_datalake():
    if DATALAKE_TYPE == "s3":
        return S3Datalake(
            bucket_name=S3_BUCKET,
            aws_access_key=AWS_ACCESS_KEY,
            aws_secret_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
    else:
        return LocalDatalake()
