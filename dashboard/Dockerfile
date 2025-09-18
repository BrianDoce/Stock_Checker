# Stock_Checker/dashboard/Dockerfile

FROM python:3.10-slim

WORKDIR /app

# Copy requirements (you might have dashboard-specific requirements.txt)
COPY ../requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# Copy dashboard code
COPY . /app

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
