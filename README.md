# SciProxy

SciProxy is an aiohttp service that resolves a DOI and serves a PDF from a
configured downloader. It tries IEEE Xplore first, then Sci-Hub. Successful
downloads are validated as PDFs before they are served or cached.

## Run

To install the dependencies into a local virtual environment and start the
server at `http://127.0.0.1:8069`:

```sh
make run
```

In another terminal, run `make smoke` to check the index and the local PDF URL
endpoint. The smoke check does not download a PDF. `make setup` installs the
dependencies with `uv` without starting the server. `make run` loads `.env` if
present. You can override `HOST` and `PORT` on the command line. The Makefile
uses port `8069` by default; direct Poetry runs use the application's default
port `8080`. The home page shows an animated progress bar while the PDF is
being found and prepared. The server sends search, download, validation, and
ready updates to the page; transfer progress is shown when available. The form
accepts DOI strings, `doi.org` links, and IEEE Xplore document links, with
cancel and retry controls. Up to four PDF transfers run at once, including
cached files. The next 16 requests wait in a queue, and further requests
receive a retryable `503` response. Concurrent requests for the same uncached
item share one fetch when caching is enabled and the PDF fits the cache limit.
These limits are applied per server process.

Alternatively, install the project with Poetry and start the server directly:

```sh
poetry install
poetry run sciproxy
```

Configuration is read from environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8080` | Listen port |
| `PROXY_URL` | unset | HTTP proxy for IEEE PDF requests |
| `IEEE_HOSTNAME` | `ieeexplore.ieee.org` | IEEE Xplore hostname |
| `CACHE_DIR` | `cache` | Directory for validated PDF files; set empty to disable caching |
| `CACHE_MAX_BYTES` | `2147483648` | Maximum total size of cached PDFs; least recently used files are removed first |
| `MAX_PDF_BYTES` | `104857600` | Maximum upstream PDF size in bytes |
| `MAX_CONCURRENT_DOWNLOADS` | `4` | Maximum active PDF transfers and uncached download jobs |
| `MAX_QUEUED_DOWNLOADS` | `16` | Maximum waiting download jobs before new requests receive `503` |
| `UNPAYWALL_EMAIL` | unset | Email required for the Unpaywall API endpoint |

Set private proxy credentials in your local `.env` file or a secret store.
The Unpaywall endpoint returns `503` until `UNPAYWALL_EMAIL` is configured.

## Endpoints

| Request | Result |
| --- | --- |
| `GET /10.1109/5.771073` | Validated PDF, or `404` when no downloader finds one |
| `GET /10.1109/5.771073?nocache` | Fetch again and replace the cached copy |
| `GET /10.1109/5.771073?api` | JSON containing the local PDF URL |
| `GET /ieee/document/10686306` | Fetch an IEEE PDF directly by document ID |
| `GET /api/10.1109/5.771073` | Unpaywall metadata with the local PDF URL added to `oa_locations` |

Cache filenames use a SHA-256 digest of the DOI or IEEE document key. The cache
is trimmed to `CACHE_MAX_BYTES` at startup and after each new PDF is stored,
evicting the least recently used cached PDFs first. Temporary download files
are staged outside the cache directory. A PDF larger than the cache limit is
sent to the requester without being cached.
