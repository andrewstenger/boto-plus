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

        start = __convert_unix_timestamp_to_utc(job_info['startedAt'])
        stop  = __convert_unix_timestamp_to_utc(job_info['stoppedAt'])
        total = stop - start

        total_seconds = total.total_seconds()

        payload = {
            'start' : start,
            'stop'  : stop,
            'total-seconds' : total_seconds,
        }

        return payload


# helpers
def __convert_unix_timestamp_to_utc(timestamp: int):
    seconds = timestamp / 1000
    utc_ts  = dt.datetime.fromtimestamp(seconds, tz=dt.timezone.utc)
    return utc_ts
