FROM python:latest
WORKDIR /db_sem6
ADD requirements.dev requirements.dev
RUN pip install -r requirements.dev
COPY main.py main.py
COPY . .
CMD ["python", "-u", "main.py"]