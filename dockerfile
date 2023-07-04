FROM  python:3.8.17-slim-bullseye
ENV PIP_NO_CACHE_DIR=yes
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
COPY env.list .
CMD ["python", "run.py"]