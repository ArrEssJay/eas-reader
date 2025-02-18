FROM python:3.11.-slim

# Set the working directory in the container
WORKDIR /app

RUN ls
# Copy the requirements file and install dependencies
COPY . .
RUN python3 -m pip install --user --no-cache-dir -r /app/requirements.txt

# Copy the rest of the application code
COPY . .

VOLUME /config

# Run the Python script located in the .devcontainer folder
CMD ["python3","-u", "./eas-reader.py"]