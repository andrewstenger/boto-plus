import datetime as dt
import boto3
import botocore

import boto_plus


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


    def get_runtime_from_batch_job(
        self,
        job_id: str,
        convert_datetime_to_string=False,
    ) -> dict:
        response = self.__batch_client.describe_jobs(
            jobs=[
                job_id,
            ]
        )

        # this should never happen -- so hard-fail if it does somehow
        if len(response['jobs']) > 1:
            error_str = f'More than one Batch jobs returned when calling "describe_jobs()" with job-id "{job_id}"'\
                ' Please investigate further.'
            raise RuntimeError(error_str)

        job_info = response['jobs'][0]

        start = dt.datetime.fromtimestamp(job_info['startedAt'] / 1000, tz=dt.timezone.utc)
        stop = dt.datetime.fromtimestamp(job_info['stoppedAt'] / 1000, tz=dt.timezone.utc)
        total = stop - start

        total_seconds = total.total_seconds()

        if convert_datetime_to_string:
            dt_format = '%Y-%m-%d %H:%M:%S.%f'
            start = start.strftime(dt_format)
            stop  = stop.strftime(dt_format)
            total_seconds = str(total_seconds)

        payload = {
            'start' : start,
            'stop' : stop,
            'total-seconds' : total_seconds,
        }

        return payload



if __name__ == '__main__':
    config = botocore.config.Config(region_name='us-east-1')
    session = boto3.session.Session()
    bp = BatchPlus(boto_config=config, boto_session=session)

    bp.get_runtime_from_batch_job(job_id='9ec5c9e9-60cc-40a5-b319-9d24616bb124')

