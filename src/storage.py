import logging

from minio import Minio

from settings import settings


def create_bucket(bucket_name: str) -> None:
    """
    Create a MinIO bucket if it doesn't already exist.

    Parameters
    ----------
    bucket_name : str
        The name of the bucket to create

    Returns
    -------
    None
    """
    client = Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_ACCESS_KEY,
        secure=False,
    )

    found = client.bucket_exists(bucket_name)

    if not found:
        client.make_bucket(bucket_name)
        logging.info(f'Created MinIO bucket "{bucket_name}"')
    else:
        logging.info(f'Bucket MinIO "{bucket_name}" already exists')
