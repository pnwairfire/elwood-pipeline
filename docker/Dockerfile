FROM python:3.12-slim
WORKDIR /app

ENV PYTHONPATH=/app

# Install dependencies needed for geopandas (such as C libraries) if needed,
# but slim Python has them or pip installs wheel/binaries. Let's run standard setup.
# Actually, pip installs binary wheels for pyproj, shapely, geopandas.
# We will copy the requirements file or pyproject dependencies.
# Wait, we can generate requirements.txt using uv or pip-compile, or write it directly.
# Let's write the requirements.txt and use it.
# Install uv from official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY pyproject.toml .
COPY elwood_pipeline/ elwood_pipeline/
RUN uv pip install --system --no-cache .

ENTRYPOINT ["python", "-m", "elwood_pipeline"]
