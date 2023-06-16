# Backup Postgres to S3
A simple script to backup a Postgres database to S3 periodically.

```bash
cp .env.example .env
```
Fill in the .env file with your credentials.

```bash
docker build -t backup .
```

```bash
docker run -e .env backup
```