import os
from sciproxy import SciProxy
from sciproxy import CAPESDownloader
from sciproxy import SciHubDownloader


def main():
    proxy_url = os.environ["PROXY_URL"]
    proxy_username = os.environ["PROXY_USERNAME"]
    proxy_password = os.environ["PROXY_PASSWORD"]
    cache_dir = os.environ.get("CACHE_DIR", None)
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))

    downloaders = [
        CAPESDownloader(
            proxy_url=proxy_url,
            proxy_auth={"username": proxy_username, "password": proxy_password},
        ),
        SciHubDownloader(),
    ]
    proxy = SciProxy(downloaders, cache_dir=cache_dir)
    proxy.run(host=host, port=port)


if __name__ == "__main__":
    main()
