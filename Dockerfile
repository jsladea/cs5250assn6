FROM python:3.12
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY main.py .
COPY test.py .
COPY test-delete-widget.json .
COPY test-update-widget.json .
COPY test-widget.json .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py", "-rq", "cs5250-requests", "-dwt", "widgets"]