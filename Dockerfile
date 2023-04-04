FROM python:3.8

WORKDIR /usr/src/app
# copy project
COPY . /usr/src/app
RUN curl -sSL https://install.python-poetry.org | python3 -
# install dependencies
RUN /root/.local/bin/poetry install
EXPOSE 8000
ENV TRANSPOSE_API_KEY=changeme
CMD ["/root/.local/bin/poetry", "run", "python", "app/app.py"]