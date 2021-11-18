FROM python:3.8

COPY . /app
RUN python -m pip install -r /app/requirements.txt

WORKDIR "/app"
EXPOSE 8501

CMD ["streamlit", "run", "app.py"]
