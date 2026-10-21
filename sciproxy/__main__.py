import os
from sciproxy import SciProxy
from sciproxy import IEEEDownloader
from sciproxy import SciHubDownloader


def main():
    proxy_url = os.environ.get("PROXY_URL")
    cache_dir = os.environ.get("CACHE_DIR", "cache") or None
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
    max_pdf_bytes = int(os.environ.get("MAX_PDF_BYTES", str(100 * 1024 * 1024)))
    cache_max_bytes = int(
        os.environ.get("CACHE_MAX_BYTES", str(2 * 1024 * 1024 * 1024))
    )
    max_concurrent_downloads = int(os.environ.get("MAX_CONCURRENT_DOWNLOADS", "4"))
    max_queued_downloads = int(os.environ.get("MAX_QUEUED_DOWNLOADS", "16"))
    proxy = SciProxy(
        downloaders,
        cache_dir=cache_dir,
        max_pdf_bytes=max_pdf_bytes,
        unpaywall_email=os.environ.get("UNPAYWALL_EMAIL"),
        max_cache_bytes=cache_max_bytes,
        max_concurrent_downloads=max_concurrent_downloads,
        max_queued_downloads=max_queued_downloads,
    )
    proxy.run(host=host, port=port)


if __name__ == "__main__":
    main()
