import datetime as dt
import boto3
import botocore


class BatchPlus:

    def __init__(
        self,
        boto_config,
        boto_session=None,
    ):
        if boto_session is not None:
            self.__batch_client = boto_session.client('batch', config=boto_config)
        else:
            self.__batch_client = boto3.client('batch', config=boto_config)


    def get_runtime_of_jobs(
        self,
        job_ids: list[str],
        convert_datetime_to_string=False,
    ) -> dict:
        map_job_id_to_runtimes = dict()

        dt_format = '%Y-%m-%d %H:%M:%S.%f'

        n = 100
        start_index = 0
        end_index = n

        job_id_slice = job_ids[:n]

        while len(job_id_slice) > 0:
            response = self.__batch_client.describe_jobs(
                jobs=job_id_slice,
            )

            for job_info in response['jobs']:
                job_id = job_info['jobId']

                start = dt.datetime.fromtimestamp(job_info['startedAt'] / 1000, tz=dt.timezone.utc)
                stop = dt.datetime.fromtimestamp(job_info['stoppedAt'] / 1000, tz=dt.timezone.utc)
                total = stop - start

                total_seconds = total.total_seconds()

                if convert_datetime_to_string:
                    start = start.strftime(dt_format)
                    stop  = stop.strftime(dt_format)
                    total_seconds = str(total_seconds)

                map_job_id_to_runtimes[job_id] = {
                    'start' : start,
                    'stop' : stop,
                    'total-seconds' : total_seconds,
                }

            start_index += n
            end_index   += n

            job_id_slice = job_ids[start_index:end_index]

        return map_job_id_to_runtimes


    def get_status_of_jobs(
        self,
        job_ids: list[str],
    ) -> dict:
        map_job_id_to_status = dict()

        n = 100
        start_index = 0
        end_index = n

        job_id_slice = job_ids[:n]

        while len(job_id_slice) > 0:
            response = self.__batch_client.describe_jobs(
                jobs=job_id_slice,
            )

            if 'jobs' not in response:
                raise RuntimeError(f'No metadata found for Batch jobs "{job_id_slice}".')

            for job_info in response['jobs']:
                job_id = job_info['jobId']
                status = job_info['status']
                map_job_id_to_status[job_id] = status

            start_index += n
            end_index   += n

            job_id_slice = job_ids[start_index:end_index]
        
        return map_job_id_to_status


if __name__ == '__main__':
    config = botocore.config.Config(region_name='us-east-1')
    session = boto3.session.Session()
    bp = BatchPlus(boto_config=config, boto_session=session)

    bp.get_runtime_from_batch_job(job_id='9ec5c9e9-60cc-40a5-b319-9d24616bb124')

