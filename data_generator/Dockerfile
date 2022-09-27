FROM python:3.8-slim-buster

RUN python -m pip install --upgrade pip

# WORKDIR /bash/sh
COPY data_generator/requirements.txt .

RUN python -m pip install -r requirements.txt

WORKDIR /opt/container/data_generator

COPY data_generator/generator_fake_data.py /opt/container/data_generator

CMD ["python", "generator_data.py"]