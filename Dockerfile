FROM python:3.11-bullseye

SHELL ["/bin/bash", "-c"]

WORKDIR /app

RUN apt-get update && \
    apt-get install -y curl postgresql lsb-release gpg && \
    curl -fsSL https://packages.redis.io/gpg | gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/redis.list && \
    apt-get update && \
    apt-get install -y redis

RUN python3 -m pip install pipx && \
    pipx ensurepath

ENV PATH="/root/.local/bin:${PATH}"
RUN pipx install poetry

RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
RUN source ~/.bashrc && nvm install 20

COPY ./backend/pyproject.toml ./backend/poetry.lock ./backend/

RUN cd backend && \
    poetry install && \
    rm -rf ~/.cache/pypoetry

COPY ./frontend/package.json ./frontend/package-lock.json ./frontend/

RUN cd frontend && \
    source ~/.bashrc && \
    npm install

COPY . .

CMD ["make", "local", "-j", "5"]
