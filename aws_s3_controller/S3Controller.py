from shining_pebbles import *
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import re
import io
import pandas as pd


def scan_files_in_bucket_subfolder_by_regex(bucket_name, file_folder_bucket, regex, option='path'):
    s3 = boto3.client('s3')
    file_folder_bucket_with_slash = file_folder_bucket + '/' if file_folder_bucket[-1] != '/' else file_folder_bucket
    pattern = re.compile(regex)
    try:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=file_folder_bucket_with_slash)
        files = []
        for page in page_iterator:
            if 'Contents' in page:
                for file in page['Contents']:
                    if pattern.search(file['Key']) and file['Key'] != file_folder_bucket_with_slash:
                        files.append(file['Key'])
        if files:
            mapping_option = {
                'name': [file.split('/')[-1] for file in files],
                'path': files
            }
            try:
                files = mapping_option[option]
            except KeyError:
                print(f"Invalid option '{option}'. Available options: {', '.join(mapping_option.keys())}")
                return []
    
            print(f"Files matching the regex '{regex}' in the bucket '{bucket_name}' with prefix '{file_folder_bucket}':")
            for file in files:
                print(file)
            return files
        else:
            print(f"No files matching the regex '{regex}' found in the bucket '{bucket_name}' with prefix '{file_folder_bucket}'")
            return []
    except NoCredentialsError:
        print("Credentials not available.")
        return []
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

def open_df_in_bucket_subfolder(bucket_name, file_folder_bucket=None, file_name=None, file_key=None):
    """
    S3 버킷의 특정 하위 폴더에서 CSV 파일을 읽어 pandas DataFrame으로 반환합니다.
    
    :param bucket_name: S3 버킷 이름
    :param file_folder_bucket: 파일이 위치한 버킷 내 폴더 경로
    :param file_name: 파일 이름
    :return: pandas DataFrame
    """
    # S3 클라이언트 생성
    s3 = boto3.client('s3')
    
    # 파일 경로 생성
    if file_folder_bucket != None and not file_folder_bucket.endswith('/'):
        file_folder_bucket += '/'
    file_path = f"{file_folder_bucket}{file_name}" if file_name != None else file_key
    
    try:
        # S3에서 파일 내용 가져오기
        content = s3.get_object(Bucket=bucket_name, Key=file_path)['Body'].read()
        
        # 내용을 DataFrame으로 변환
        df = pd.read_csv(io.BytesIO(content))
        
        print(f"Successfully read file: {file_path}")
        print(f"DataFrame shape: {df.shape}")
        
        return df
    
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")
        return None


def open_df_in_bucket_subfolder_by_regex(bucket_name, file_folder_bucket, regex, index=-1):
    file_keys = scan_files_in_bucket_subfolder_by_regex(bucket_name, file_folder_bucket, regex, option='path')
    file_key = file_keys[index]
    df = open_df_in_bucket_subfolder(bucket_name, file_key=file_key)
    return df


def copy_files_to_another_bucket(source_bucket, destination_bucket, source_prefix, destination_prefix):
    # S3 클라이언트 초기화
    s3 = boto3.client('s3')

    try:
        # 원본 버킷의 프리픽스로 시작하는 파일 목록을 가져옴
        objects = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)
        
        if 'Contents' in objects:
            for obj in objects['Contents']:
                # 파일 이름만 추출 (전체 키에서 프리픽스 제거)
                file_name = obj['Key'][len(source_prefix):]

                # 목적지의 전체 파일 경로 생성
                destination_key = destination_prefix + file_name
                
                # 파일을 다른 버킷으로 복사
                copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}
                s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
                print(f"File {obj['Key']} copied to {destination_key} in {destination_bucket}")
        else:
            print(f"No files found in {source_bucket}/{source_prefix}")

    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(e)

def download_files_from_s3(bucket_name, file_folder_bucket=None, file_folder_local=None):
    file_folder_bucket = file_folder_bucket or ''
    file_folder_local = file_folder_local or file_folder_bucket
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_folder_bucket)
    files = [file for file in response.get('Contents', []) if not file['Key'].endswith('/')]

    if not files:
        print(f'No files found in the folder "{file_folder_bucket}" in bucket "{bucket_name}"')
        return

    if not os.path.exists(file_folder_local):
        os.makedirs(file_folder_local)

    for file in files:
        key = file['Key']
        file_name = os.path.basename(key)
        local_file_path = os.path.join(file_folder_local, file_name)
        s3.download_file(bucket_name, key, local_file_path)
        print(f'Downloaded file: {file_name}')


def upload_files_to_s3(bucket_name, file_folder_local):
    # 로컬 폴더 내의 파일 목록 가져오기
    files = os.listdir(file_folder_local)
    
    # AWS S3 클라이언트 생성
    s3 = boto3.client('s3')
    
    # 각 파일을 S3 버킷으로 업로드
    for file_name in files:
        local_file_path = os.path.join(file_folder_local, file_name)
        s3_key = file_name  # 파일 이름을 S3 키로 사용
        s3.upload_file(local_file_path, bucket_name, s3_key)
        
        # 업로드 성공 여부 확인 및 로그 출력
        try:
            s3.head_object(Bucket=bucket_name, Key=s3_key)
            print(f"{file_name} 업로드 완료")
        except Exception as e:
            print(f"{file_name} 업로드 실패: {e}")

def create_subfolder_in_bucket(bucket_name, subfolder_name):
    if subfolder_name[-1] != '/':
        subfolder_name += '/'
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, subfolder_name).put(Body=b'')
    print(f"Subfolder '{subfolder_name}' created in bucket '{bucket_name}'.")


def scan_files_with_text_in_bucket(bucket_name, text, prefix=''):
    s3 = boto3.client('s3')
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = [file['Key'] for file in response.get('Contents', []) if text in file['Key']]
        
        bucket = []
        if files:
            print(f"Files containing '{text}' in their names:")
            for file in files:
                bucket.append(file)
                # print(file)
            return bucket
        else:
            print(f"No files containing '{text}' found in the bucket '{bucket_name}' with prefix '{prefix}'")
    except Exception as e:
        print(f"An error occurred: {e}")


def scan_files_in_folder_in_bucket_by_regex(bucket_name, file_folder_bucket, regex):
    s3 = boto3.client('s3')
    pattern = re.compile(regex)
    try:
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix='')
        
        files = []
        
        for page in page_iterator:
            if 'Contents' in page:
                for file in page['Contents']:
                    if pattern.search(file['Key']):
                        files.append(file['Key'])
        
        if files:
            print(f"Files matching the regex '{regex}' in the bucket '{bucket_name}' with prefix '{file_folder_bucket}':")
            for file in files:
                print(file)
            return files
        else:
            print(f"No files matching the regex '{regex}' found in the bucket '{bucket_name}' with prefix '{file_folder_bucket}'")
            return []
    except NoCredentialsError:
        print("Credentials not available.")
        return []
    except PartialCredentialsError:
        print("Incomplete credentials provided.")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []

# 사용 예제
# regex = r'your_regex_pattern_here'
# scan_files_in_subfolder_in_bucket_by_regex('your_bucket_name', 'your_file_folder_bucket', regex)


def move_files_between_s3_buckets(source_bucket, source_folder, destination_bucket, destination_folder, regex):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_folder)
    regex = re.compile(regex)

    if 'Contents' not in response:
        print(f'No files found in the folder "{source_folder}" in bucket "{source_bucket}"')
        return

    files_to_move = [file['Key'] for file in response['Contents'] if regex.search(file['Key'])]
    
    if not files_to_move:
        print(f'No files matching the pattern "{regex}" found in the folder "{source_folder}"')
        return
    
    for key in files_to_move:
        copy_source = {'Bucket': source_bucket, 'Key': key}
        destination_key = key.replace(source_folder, destination_folder, 1) if source_folder != './' else destination_folder + key

        try:
            # Copy the file to the destination bucket
            s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
            # Delete the file from the source bucket
            s3.delete_object(Bucket=source_bucket, Key=key)
            print(f'Moved file: {key} to {destination_key}')
        except Exception as e:
            print(f'Failed to move file {key}: {e}')


def move_files_to_another_subfolder_in_bucket(bucket_name, files, subfolder_name):
    s3 = boto3.client('s3')
    if subfolder_name[-1] != '/':
        subfolder_name += '/'
    
    # Check if the subfolder exists
    subfolder_exists = s3.list_objects_v2(Bucket=bucket_name, Prefix=subfolder_name, MaxKeys=1)
    if not subfolder_exists.get('Contents'):
        raise Exception(f"Subfolder '{subfolder_name}' does not exist in the bucket '{bucket_name}'.")

    for file_key in files:
        new_key = subfolder_name + os.path.basename(file_key)
        copy_source = {'Bucket': bucket_name, 'Key': file_key}
        
        try:
            s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=new_key)
            s3.delete_object(Bucket=bucket_name, Key=file_key)
            print(f"Moved {file_key} to {new_key}")
        except Exception as e:
            print(f"Failed to move {file_key} to {new_key}: {e}")


def merge_timeseries_csv_files(file_path_old, file_path_new, file_name_save=None, file_folder_save=None):
    try:
        old_data = pd.read_csv(file_path_old, dtype=str)
        new_data = pd.read_csv(file_path_new, dtype=str)
    except Exception as e:
        print(f"Error reading files: {e}")
        return

    if old_data.empty:
        raise ValueError(f"The file {file_path_old} is empty.")
    if new_data.empty:
        raise ValueError(f"The file {file_path_new} is empty.")

    date_column = '일자'

    old_data[date_column] = pd.to_datetime(old_data[date_column])
    new_data[date_column] = pd.to_datetime(new_data[date_column])

    old_last_date = old_data[date_column].iloc[-1]
    new_data_to_add = new_data[new_data[date_column] > old_last_date]

    old_data[date_column] = old_data[date_column].dt.strftime('%Y-%m-%d')
    new_data_to_add[date_column] = new_data_to_add[date_column].dt.strftime('%Y-%m-%d')

    combined_data = pd.concat([old_data, new_data_to_add])

    first_date = combined_data[date_column].iloc[1]
    last_date = combined_data[date_column].iloc[-1]

    # 파일명 형식을 유지하여 새로운 파일명 생성
    old_file_name = os.path.basename(file_path_old)
    base_file_name = old_file_name.split('-to')[0]  # 'menu2160-code100060'
    base_menu_code = base_file_name.split('-')[0]
    file_name_save = file_name_save if file_name_save else f"{base_file_name}-to{last_date.replace('-', '')}-save{get_today('%Y%m%d')}.csv"

    # 저장할 폴더가 지정되지 않은 경우, 현재 폴더에 저장
    if not file_folder_save:
        base_folder_name = f"dataset-timeseries-{base_menu_code}-from{first_date.replace('-', '')}-to{last_date.replace('-', '')}-merge{get_today('%Y%m%d')}"
        file_folder_save = os.path.join('.', base_folder_name)
    
    check_folder_and_create_folder(file_folder_save)
    file_path_save = os.path.join(file_folder_save, file_name_save)
    combined_data.to_csv(file_path_save, index=False)
    print(f"Merged file saved as {file_path_save}")
    
    return combined_data