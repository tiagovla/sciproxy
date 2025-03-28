from typing import Optional


import logging

log = logging.getLogger(__name__)


class Proxy:
    """
    Represent a proxy configuration, including hostname, port, and optional authentication.

    Attributes:
        hostname (str): The hostname of the proxy server.
        port (int): The port number of the proxy server.
        username (Optional[str]): The username for proxy authentication, if required.
        password (Optional[str]): The password for proxy authentication, if required.
        proxy_url (str): The base URL of the proxy server.
    """

    def __init__(
        self,
        hostname: str,
        port: int | str,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        """
        Initialize a Proxy instance.

        Args:
            hostname (str): The hostname of the proxy server.
            port (int | str): The port number of the proxy server.
            username (Optional[str], optional): The username for proxy authentication. Defaults to None.
            password (Optional[str], optional): The password for proxy authentication. Defaults to None.
        """
        self.hostname: str = hostname
        self.port: int = int(port)
        self.username: Optional[str] = username
        self.password: Optional[str] = password
        self.proxy_url: str = f"http://{self.hostname}:{self.port}"

    @property
    def url(self) -> str:
        """
        Construct the full proxy URL, including authentication if provided.

        Returns:
            str: The full proxy URL.
        """
        if self.username and self.password:
            return f"http://{self.username}:{self.password}@{self.hostname}:{self.port}"
        return f"http://{self.hostname}:{self.port}"

    @classmethod
    def from_url(cls, url: str) -> "Proxy":
        """
        Create a Proxy instance from a URL string.

        Args:
            url (str): The proxy URL string.

        Returns:
            Proxy: A Proxy instance parsed from the URL.
        """
        url = url.replace("http://", "").replace("https://", "")
        if "@" in url:
            auth, url = url.split("@")
            username, password = auth.split(":")
        else:
            username = password = None
        hostname, port = url.split(":")
        return cls(hostname, port, username, password)

    def __repr__(self) -> str:
        """
        Return a string representation of the Proxy instance.

        Returns:
            str: A string representation of the Proxy instance.
        """
        if self.username and self.password:
            return f"Proxy(hostname: {self.hostname}, port: {self.port}, username: {self.username}, password: {self.password})"
        return f"Proxy(hostname: {self.hostname}, port: {self.port})"

    def __str__(self) -> str:
        """
        Return a human-readable string representation of the Proxy instance.

        Returns:
            str: A human-readable string representation of the Proxy instance.
        """
        return self.__repr__()
