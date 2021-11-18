FROM python:3.8

COPY . /app
RUN python -m pip install -r /app/requirements.txt

WORKDIR "/app"
EXPOSE 8000

CMD ["uvicorn", "app:app", "--reload", "--host", "0.0.0.0"]
