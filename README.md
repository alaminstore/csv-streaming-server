# CSV Streaming Server

NestJS backend for streaming large CSV imports with real-time progress updates.

## Tech Stack

- NestJS
- Prisma + MongoDB
- BullMQ + Redis
- Server-Sent Events (SSE)

## Setup

```bash
# Install dependencies
npm install

# Generate Prisma client
npx prisma generate

# Push schema to database
npx prisma db push
```

# Create .env file

cp .env.example .env

```
DATABASE_URL="mongodb://localhost:27017/csv_streaming?replicaSet=rs0"
REDIS_HOST="localhost"
REDIS_PORT=6379
CSV_FILE_PATH="./data/customers.csv"
```

## Run

```bash
# Development
npm run start:dev

# Production
npm run build
npm run start:prod
```

## Features

- CSV streaming with backpressure handling
- Background job processing (BullMQ)
- Real-time progress via SSE
- Resume support after restart
- Customer CRUD API

## What has been completed and what is pending
- I have tried to cover all the requirements and also address the bonus points (I Will cover it later).
- Frontend Issue: Pagination not updated without refresh (I'll solve it later)

## Frontend
- [csv-streaming-client](https://github.com/alaminstore/csv-streaming-client/tree/main)
