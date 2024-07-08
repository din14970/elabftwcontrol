from typing import Optional

from elabftwcontrol._logging import logger
from elabftwcontrol.client import ElabftwApi


class GlobalConfig:
    """A class to make it to configure and store a global connection instead of being forced to always create and pass a client"""

    _client: Optional[ElabftwApi] = None

    @classmethod
    def get_client(
        cls,
    ) -> ElabftwApi:
        if cls._client is None:
            raise RuntimeError(
                "No connection has been configured. Please use the `elabftwcontrol.connect` "
                "function to set up a connection."
            )
        return cls._client

    @classmethod
    def configure_client(
        cls,
        profile: str = "default",
        host_url: Optional[str] = None,
        api_key: Optional[str] = None,
        verify_ssl: Optional[bool] = None,
    ) -> None:
        if host_url is not None:
            client = ElabftwApi.new(
                host_url=host_url,
                api_key=api_key,
                verify_ssl=verify_ssl,
            )
        else:
            logger.info(f"No host url configured, trying to use profile: {profile}")
            try:
                client = ElabftwApi.from_config_file(profile=profile)
            except KeyError:
                raise RuntimeError(
                    f"Profile `{profile}` is not configured. Use `elabftwctl config` "
                    "on the command line to set up a new profile."
                )
            except Exception as e:
                raise RuntimeError(f"Unexpected error: {e}")
        cls._client = client

    @classmethod
    def reset_client(
        cls,
    ) -> None:
        cls._client = None

    @classmethod
    def test_connection(cls) -> None:
        client = cls.get_client()
        info = client.info
        try:
            info.get_info()
        except Exception as e:
            raise RuntimeError(f"Could not establish connection to eLabFTW: {e}")
        logger.info(f"Succesfully connected to eLabFTW host: {client.host_name}")


def connect(
    profile: str = "default",
    host_url: Optional[str] = None,
    api_key: Optional[str] = None,
    verify_ssl: Optional[bool] = None,
    test_connection: bool = True,
) -> None:
    GlobalConfig.configure_client(
        profile=profile,
        host_url=host_url,
        api_key=api_key,
        verify_ssl=verify_ssl,
    )
    if test_connection:
        GlobalConfig.test_connection()
