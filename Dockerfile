FROM python:3.11
ENV APP_HOME="/app"
WORKDIR ${APP_HOME}

COPY pyproject.toml poetry.lock docker-entrypoint.sh ./
COPY scripts ./scripts
COPY utils ./utils

RUN pip install --upgrade pip
RUN curl -sSL https://install.python-poetry.org | python3 -
RUN $HOME/.local/bin/poetry config virtualenvs.create false && \
    $HOME/.local/bin/poetry install --no-dev --no-ansi --no-interaction

ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["default"]