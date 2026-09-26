# Shared configuration

## What it does

`shared/config.py` defines the settings used by the services, simulators, and
dashboards. It keeps all environment-variable names and local defaults in one
small module.

## Data flow

Each process calls `load_settings()` when it starts. The function reads Kafka,
PostgreSQL, ingestion, storage metrics, sampling, and Kubernetes values from
the process environment. Missing values receive development defaults, and the
function returns one immutable `Settings` object.

## Important parts

- `Settings` lists every supported configuration value in a frozen dataclass.
- `load_settings()` reads environment variables and converts numeric values to
  `int` or `float`.

## Dependencies

This module uses only Python's `os` and `dataclasses` modules. See
`.env.example`, `compose.yaml`, and `kubernetes/configmaps.yaml` for the values
used in each environment. The module does not load `.env` files itself.

## How to run it

It is a library and is not run as a separate process. Other scripts import it:

```python
from shared.config import load_settings

settings = load_settings()
```

Set configuration in the shell, Docker Compose, or Kubernetes before starting
the process that imports it.

## What to understand

The same application code works in local, Docker, and Kubernetes environments
because only the environment values change. A configuration framework is not
needed for this small project.
