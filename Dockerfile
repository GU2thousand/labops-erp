FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . .
RUN LABOPS_DB_MODE=sqlite-demo LABOPS_DEBUG=0 LABOPS_SECRET_KEY=build-static-only python manage.py collectstatic --noinput \
    && useradd --uid 10001 --create-home labops \
    && chmod +x infra/entrypoint.sh
USER labops
ENTRYPOINT ["/app/infra/entrypoint.sh"]
CMD ["web"]
