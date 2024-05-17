import boto3
import botocore


class SSOPlus:

    def __init__(
        self,
        boto_config,
        boto_session=None,
    ):
        self.__boto_config = boto_config
        self.__boto_session = boto_session

        if boto_session is not None:
            self.__sso_client = self.__boto_session.client('sso', config=self.__boto_config)
        else:
            self.__sso_client = boto3.client('sso', config=self.__boto_config)


    def refresh_credentials(
        self,
        boto_session=None,
        do_self_refresh=True,
    ):
        if boto_session is None:
            boto_session = self.__boto_session

        #sso_cache = boto_session.get_component('credential_provider').get_provider('sso').load()
        refreshable_credentials = botocore.credentials.RefreshableCredentials.create_from_metadata(
            metadata=self._refresh(),
            refresh_using=self._refresh,
            method='sso',
        )
        boto_session._credentials = refreshable_credentials
        refreshed_session = boto3.Session(botocore_session=boto_session)
        if do_self_refresh:
            self.__boto_session = refreshed_session
            self.__sso_client = self.__boto_session.client('sso', config=self.__boto_config)

        return refreshed_session


    def _refresh(self):
        # Here you need to implement the logic to refresh your credentials
        # This usually involves calling an internal endpoint or service that
        # can issue new SSO tokens based on your current session
        new_credentials = {}  # Replace with your refresh logic
        return {
            'access_key': new_credentials['accessKeyId'],
            'secret_key': new_credentials['secretAccessKey'],
            'token': new_credentials['sessionToken'],
            'expiry_time': new_credentials['expiration'],
        }

    """
    def refresh_session(
        self,
        boto_session=None,
        do_self_refresh=True,
    ):
        if boto_session is None:
            boto_session = self.__boto_session

        credentials = boto_session.get_credentials()
        refreshable_credentials = botocore.credentials.RefreshableCredentials.create_from_metadata(
            metadata=credentials.get_frozen_credentials(),
            refresh_using=boto_session.get_credentials,
            method='sso',
        )
        botocore_session = botocore.session.get_session()
        botocore_session._credentials = refreshable_credentials
        refreshed_session = boto3.Session(botocore_session=botocore_session)

        if do_self_refresh:
            self.__boto_session = refreshed_session
            self.__sso_client = self.__boto_session.client('sso', config=self.__boto_config)

        return refreshed_session
    """