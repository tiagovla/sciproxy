import os
from sciproxy import SciProxy
from sciproxy import IEEEDownloader
from sciproxy import SciHubDownloader


def main():
    proxy_url = os.environ["PROXY_URL"]
    cache_dir = os.environ.get("CACHE_DIR", None)
    ieee_hostname = os.environ.get("IEEE_HOSTNAME", "ieeexplore.ieee.org")
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))

    downloaders = [
        IEEEDownloader(
            hostname=ieee_hostname,
            proxy_url=proxy_url,
        ),
        SciHubDownloader(),
    ]
    proxy = SciProxy(downloaders, cache_dir=cache_dir)
    proxy.run(host=host, port=port)


if __name__ == "__main__":
    main()
