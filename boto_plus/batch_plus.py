import datetime as dt
import boto3

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
    ) -> dict:
        response = self.__batch_client.describe_jobs(
            jobs=[
                job_id,
            ]
        )

        job_info = response['jobs'][0]

        start = dt.datetime.fromtimestamp(job_info['startedAt'] / 1000, tz=dt.timezone.utc)
        stop  = dt.datetime.fromtimestamp(job_info['stoppedAt'] / 1000, tz=dt.timezone.utc)
        total = stop - start

        total_seconds = total.total_seconds()

        payload = {
            'start' : start.strftime("%Y-%m-%d %H:%M:%S"),
            'stop'  : stop.strftime("%Y-%m-%d %H:%M:%S"),
            'total-seconds' : total_seconds,
        }

        return payload
