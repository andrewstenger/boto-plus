import os
import hashlib

import boto_plus


def get_bucket_and_key_from_s3_uri(
    uri: str,
):
    bucket = uri.replace('s3://', '').split('/')[0]
    key    = '/'.join(uri.replace('s3://', '').split('/')[1:])

    return bucket, key


def get_filepaths_in_directory(
    local_directory: str,
    recursive=False,
):
    if recursive:
        filepaths = [os.path.join(root, filename) for root, dirs, files in os.walk(local_directory) for filename in files]
    else:
        filepaths = [os.path.join(local_directory, filename) for filename in os.listdir(local_directory)]

    return filepaths


def is_windows_filepath(
    filepath: str,
):
    if '\\' in filepath:
        return True
    else:
        return False


def is_posix_filepath(
    filepath: str,
):
    if '/' in filepath:
        return True
    else:
        return False


def convert_filepath_to_posix(
    filepath: str,
):
    # Windows filepath provided
    if '\\' in filepath:
        # drive is specified -- it must be removed
        if ':' in filepath:
            idx = filepath.index(':')
            filepath_no_drive = filepath[idx+1:]
        else:
            filepath_no_drive = filepath

        output_filepath = filepath_no_drive.replace('\\', '/')
        
    else:
        output_filepath = filepath

    return output_filepath


def convert_filepath_to_windows(
    filepath: str,
):
    # posix filepath provided
    if '/' in filepath:
        output_filepath = filepath.replace('/', '\\')
        
    else:
        output_filepath = filepath

    return output_filepath


def get_local_file_hash(filepath, chunk_size=4096):
    if os.path.isfile(filepath):
        hash_md5 = hashlib.md5()
        with open(filepath, 'rb') as file:
            for chunk in iter(lambda: file.read(chunk_size), b''):
                hash_md5.update(chunk)

        local_file_hash = hash_md5.hexdigest()

    else:
        raise RuntimeError(f'Provided file "{filepath}" does not exist.')

    return local_file_hash


def create_textfile(
    content: str,
    filepath: str,
):
    with open(filepath, 'w+') as out_file:
        out_file.write(content)


def get_textfile_content(
    filepath: str,
):
    with open(filepath, 'r+') as in_file:
        content = in_file.read()

    return content
