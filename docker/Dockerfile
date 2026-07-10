FROM python:3.12-slim
WORKDIR /app

ENV PYTHONPATH=/app

# Install dependencies needed for geopandas (such as C libraries) if needed,
# but slim Python has them or pip installs wheel/binaries. Let's run standard setup.
# Actually, pip installs binary wheels for pyproj, shapely, geopandas.
# We will copy the requirements file or pyproject dependencies.
# Wait, we can generate requirements.txt using uv or pip-compile, or write it directly.
# Let's write the requirements.txt and use it.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY elwood_pipeline/ elwood_pipeline/

ENTRYPOINT ["python", "-m", "elwood_pipeline"]
