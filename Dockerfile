FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml ./
COPY shared ./shared
COPY services ./services
COPY dashboard ./dashboard
COPY scripts ./scripts

RUN pip install --no-cache-dir .


FROM runtime AS test

COPY tests ./tests
RUN pip install --no-cache-dir ".[dev]"

CMD ["pytest", "-q", "-p", "no:cacheprovider"]


FROM runtime AS app

CMD ["python", "--version"]

