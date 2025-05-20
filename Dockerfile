FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

RUN ls
# Copy the requirements file and install dependencies
COPY . .
RUN curl -sSL https://install.python-poetry.org | python3 -

# Set up poetry virtual environment
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi  --no-root

# Copy the rest of the application code
COPY . .

VOLUME /config

# Run the Python script located in the .devcontainer folder
CMD ["python3","-u", "./eas-reader.py"]