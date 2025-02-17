FROM python:3.13.2-alpine3.21 AS builder

ARG POETRY_VERSION=1.8.5
ENV POETRY_HOME=/opt/poetry
ENV POETRY_NO_INTERACTION=1
ENV POETRY_VIRTUALENVS_IN_PROJECT=1
ENV POETRY_VIRTUALENVS_CREATE=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV POETRY_CACHE_DIR=/opt/.cache

RUN pip install --no-cache-dir "poetry==${POETRY_VERSION}"

WORKDIR /app

COPY pyproject.toml poetry.lock README.md /app/

RUN poetry install --only main --no-root && rm -rf $POETRY_CACHE_DIR

COPY sciproxy /app/sciproxy

RUN poetry install

FROM python:3.13.2-alpine3.21 AS runtime

ENV PATH="/app/.venv/bin:$PATH"

COPY --from=builder /app /app

EXPOSE 8080
#
CMD  ["sciproxy"]
