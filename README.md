# SciProxy

[![Python 3.13+](https://img.shields.io/badge/Python-3.13%2B-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![GitHub stars](https://img.shields.io/github/stars/tiagovla/sciproxy?style=flat-square)](https://github.com/tiagovla/sciproxy/stargazers)
[![Last commit](https://img.shields.io/github/last-commit/tiagovla/sciproxy?style=flat-square)](https://github.com/tiagovla/sciproxy/commits)

SciProxy is a self-hosted web service for finding and serving research PDFs. Enter a DOI, a
`doi.org` link, or an IEEE Xplore document link. SciProxy tries IEEE Xplore first and then Sci-Hub,
validates the result as a PDF, and streams it to the browser.

The web page reports search and preparation progress, shows transfer progress when available, and
lets you cancel or retry a request. The server also provides a DOI API and a bounded local PDF
cache.

## Run locally

Install [uv](https://docs.astral.sh/uv/), create a virtual environment, and install SciProxy:

```sh
uv venv
uv pip install --python .venv/bin/python -e .
```

To load optional settings from `.env` and start the server at <http://127.0.0.1:8069>:

```sh
set -a
if [ -f .env ]; then . ./.env; fi
set +a
HOST=127.0.0.1 PORT=8069 .venv/bin/python -m sciproxy
```

To check the home page and API response without downloading a PDF, run these commands in another
terminal:

```sh
curl --fail --silent --show-error --output /dev/null http://127.0.0.1:8069/
curl --fail --silent --show-error 'http://127.0.0.1:8069/10.1109/5.771073?api'
```

## Run with Docker Compose

```sh
docker compose up --build
```

Open <http://127.0.0.1:3333>. Compose reads overrides from the project `.env` file and stores
cached PDFs in `./data`. Stop the service with `docker compose down`.

## Configuration

For local runs, the command above sources `.env` as shell assignments. Docker Compose reads `.env`
automatically. Keep proxy credentials and other private values there rather than in
`docker-compose.yml`.

| Variable | Application default | Purpose |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | Address the server listens on |
| `PORT` | `8080` | Server listen port |
| `PROXY_URL` | unset | Optional HTTP proxy for IEEE PDF requests |
| `IEEE_HOSTNAME` | `ieeexplore.ieee.org` | IEEE Xplore hostname |
| `CACHE_DIR` | `cache` | Directory for validated PDFs; set empty to disable caching |
| `CACHE_MAX_BYTES` | `2147483648` | Maximum cache size in bytes; least recently used files are evicted first |
| `MAX_PDF_BYTES` | `104857600` | Maximum accepted upstream PDF size in bytes |
| `MAX_CONCURRENT_DOWNLOADS` | `4` | Maximum active PDF requests |
| `MAX_QUEUED_DOWNLOADS` | `16` | Additional requests allowed to wait; excess requests receive `503` with `Retry-After` |
| `UNPAYWALL_EMAIL` | unset | Email required by the Unpaywall API endpoint; that endpoint returns `503` until configured |

The Compose configuration uses `/cache` for `CACHE_DIR`, listens on container port `8080` by
default, and preserves the IEEE hostname currently configured in `docker-compose.yml`. Set any of
these variables in `.env` to override them.

## HTTP endpoints

| Request | Result |
| --- | --- |
| `GET /10.1109/5.771073` | Validated PDF, or `404` if no downloader finds one |
| `GET /10.1109/5.771073?nocache` | Fetch again and replace the cached copy |
| `GET /10.1109/5.771073?api` | JSON with the local PDF URL |
| `GET /ieee/document/10686306` | Fetch an IEEE PDF directly by document ID |
| `GET /api/10.1109/5.771073` | Unpaywall metadata with the local PDF URL added to `oa_locations` |

The browser form also accepts complete IEEE Xplore document links, such as
`https://ieeexplore.ieee.org/document/10686306`.

## Cache and request limits

The cache defaults to a 2 GiB maximum. It is trimmed at startup and after each stored PDF, removing
the least recently used files first. A valid PDF larger than the cache limit is served but not
cached. Cache filenames use a SHA-256 digest of the DOI or IEEE document key, and temporary
downloads are staged outside the cache directory.

Each server process handles up to four PDF requests at a time and queues up to 16 more. Requests
beyond that limit receive a retryable `503` response. Concurrent requests for the same uncached
item share one fetch when caching is enabled and the result fits in the cache. These limits apply
per server process.
