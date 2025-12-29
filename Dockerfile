FROM python:3.12-bookworm AS build

WORKDIR /opt/sqlanywhere-install
COPY client17011 .
ENV PATH=${PATH}:/opt/sqlanywhere-install/bin64
ENV SQLANY17=/opt/sqlanywhere17
ENV LD_LIBRARY_PATH="$SQLANY17/lib64:$SQLANY17/lib32"
RUN ./setup -silent -I_accept_the_license_agreement
WORKDIR /
RUN rm -rf /opt/sqlanywhere-install


RUN pip install pip --upgrade
RUN pip install poetry

ENV POETRY_VIRTUALENVS_CREATE=true \
    POETRY_VIRTUALENVS_IN_PROJECT=true

# Copy project files and install dependencies
WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN poetry install --no-root --only main

## Stage 2: Runtime Stage
FROM python:3.12-slim-bookworm AS runtime

# Copy SQL Anywhere installation from build stage
COPY --from=build /opt/sqlanywhere17 /opt/sqlanywhere17

# Set environment variables for SQL Anywhere and Poetry venv
ENV SQLANY17=/opt/sqlanywhere17 \
    LD_LIBRARY_PATH="/opt/sqlanywhere17/lib64" \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app
COPY --from=build /app/.venv .venv
COPY sybase_postgres_sync.py . 
# Create a non-root user for security
RUN useradd -U -M -d /nonexistent app
USER app
#
# Run the sync by default
CMD ["python", "sybase_postgres_sync.py"]
