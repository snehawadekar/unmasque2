# For more information, please refer to https://aka.ms/vscode-docker-python


FROM python:3
# FROM postgres

EXPOSE 8080

RUN python -m pip install --upgrade pip
RUN pip install Flask
RUN pip install gunicorn flask
RUN wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy
RUN chmod +x cloud_sql_proxy
RUN pip install 'cloud-sql-python-connector[pg8000]'
# RUN pip install google.cloud.sql.connector
WORKDIR /services/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY ./requirements.txt /services/app/requirements.txt
WORKDIR /services/app
RUN pip install -r requirements.txt
COPY . /services/app/


# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["gunicorn", "main:app","--timeout","60"]


# ENV POSTGRES_USER postgres
# For more information, please refer to https://aka.ms/vscode-docker-python
# FROM python:3.7

# EXPOSE 8080

# # set environment variables
# ENV PYTHONDONTWRITEBYTECODE 1
# ENV PYTHONUNBUFFERED 1



# # Install pip requirements
# RUN python -m pip install --upgrade pip
# RUN pip install gunicorn flask


# COPY requirements.txt /app/requirements.txt
# WORKDIR /app
# RUN pip install -r requirements.txt
# COPY . /app

# # Creates a non-root user with an explicit UID and adds permission to access the /app folder
# # For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
# RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
# USER appuser

# # During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
# CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]

