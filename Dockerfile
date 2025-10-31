FROM python:3.12-bookworm AS build

WORKDIR /opt/sqlanywhere-install
COPY client17011 .
ENV PATH=${PATH}:/opt/sqlanywhere-install/bin64
ENV SQLANY17=/opt/sqlanywhere17
ENV LD_LIBRARY_PATH="$SQLANY17/lib64:$SQLANY17/lib32"
RUN ./setup -silent -I_accept_the_license_agreement
WORKDIR /
RUN rm -rf /opt/sqlanywhere-install

# Install Poetry using pipx for isolation
RUN pip install pipx && pipx ensurepath
RUN pipx install poetry

WORKDIR /usr/src/app
ENV POETRY_VIRTUALENVS_CREATE=true \
    POETRY_VIRTUALENVS_IN_PROJECT=true

# Copy project files and install dependencies
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --only main

# Stage 2: Runtime Stage
FROM python:3.12-slim-bookworm AS runtime

# Set PATH to include the Poetry-managed virtual environment
ENV PATH="/app/.venv/bin:${PATH}"

WORKDIR /app
COPY --from=build /app ./
RUN mkdir -p /app/logs
# Create a non-root user for security
RUN useradd -U -M -d /nonexistent app
USER app

# Run the sync by default
CMD ["python", "sybase_postgres_sync.py"]