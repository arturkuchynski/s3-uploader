import argparse
import os
import mimetypes
from pathlib import Path

import boto3
from tqdm import tqdm

BASE_DIR = Path(__file__).resolve().parent
ALLOWED_VISIBILITIES = ['private', 'public-read']


def path_object(value):
    abspath = BASE_DIR.joinpath(value)
    if abspath.exists():
        return abspath
    else:
        raise argparse.ArgumentTypeError(f'Unable to resolve path `{abspath}`. Path argument must be a valid path.')


def parse_args():
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--s3_access_key', help='S3 storage access key.', required=True)
    args_parser.add_argument('--s3_secret', help='S3 storage secret key.', required=True)
    args_parser.add_argument('--s3_endpoint', help='Endpoint of s3 storage.', required=True)
    args_parser.add_argument('--s3_region', help='Region of s3 storage.', required=True)
    args_parser.add_argument('--s3_bucket', help='Name of a bucket.', required=True)
    args_parser.add_argument('--s3_path', help='Path to store file at.', required=True)
    args_parser.add_argument('--path', help='Path to file or directory to upload.', required=True, type=path_object)
    args_parser.add_argument('--s3_root', help='Prefix (Root s3 bucket directory).', required=False, default=None)
    args_parser.add_argument('--s3_visibility',
                             help=f"File visibility level. Can be one of: {'/'.join(ALLOWED_VISIBILITIES)}.",
                             choices=ALLOWED_VISIBILITIES,
                             default=ALLOWED_VISIBILITIES[0]
                             )

    return args_parser.parse_args()


def get_s3_client(args):
    client = boto3.client(
        's3',
        region_name=args.s3_region,
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=args.s3_access_key,
        aws_secret_access_key=args.s3_secret
    )
    return client


def upload_file(input_arguments):
    client = get_s3_client(input_arguments)
    bytes_total = os.stat(input_arguments.path).st_size
    bytes_transferred = 0

    with tqdm(total=bytes_total, unit='B', unit_scale=True, unit_divisor=1024) as progress_bar:
        prefix = input_arguments.s3_root
        store_at = input_arguments.s3_path.strip('/')
        if prefix:
            store_at = f"{prefix}/{store_at}"

        store_at = f"{store_at}/{os.path.basename(input_arguments.path)}"
        mimetype, _ = mimetypes.guess_type(input_arguments.path)

        if mimetype is None:
            raise Exception(f"Failed to guess mimetype of `{input_arguments.path}`")

        def bytes_count(size):
            nonlocal bytes_transferred, progress_bar
            bytes_transferred += size
            progress_bar.update(bytes_transferred)

        client.upload_file(
            Filename=str(input_arguments.path),
            Bucket=input_arguments.s3_bucket,
            Key=store_at,
            Callback=bytes_count,
            ExtraArgs={'ACL': input_arguments.s3_visibility, 'ContentType': mimetype}
        )


def upload_dir(input_arguments):
    client = get_s3_client(input_arguments)
    prefix = input_arguments.s3_root
    store_at = input_arguments.s3_path.strip('/')
    if prefix:
        store_at = f"{prefix}/{store_at}"

    files = [path for path in input_arguments.path.rglob('*') if path.is_file()]

    for filepath in tqdm(files, unit='files'):
        key = f"{store_at}/{filepath.relative_to(input_arguments.path).as_posix()}"
        mimetype, _ = mimetypes.guess_type(filepath)

        if mimetype is None:
            raise Exception(f"Failed to guess mimetype of `{filepath}`")

        client.upload_file(
            Filename=str(filepath),
            Bucket=input_arguments.s3_bucket,
            Key=key,
            ExtraArgs={'ACL': input_arguments.s3_visibility, 'ContentType': mimetype}
        )


def main():
    args = parse_args()
    if args.path.is_dir():
        upload_dir(args)
    else:
        upload_file(args)
    print('Done.')


if __name__ == '__main__':
    main()
