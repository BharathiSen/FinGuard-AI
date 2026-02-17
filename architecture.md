# FinGuard AI - System Architecture

This document outlines the production-grade architecture for FinGuard AI, an autonomous invoice risk and fraud detection system.

## High-Level Architecture Diagram

```mermaid
graph TB
    %% Users and Frontend
    User(Finance Executive) -->|HTTPS / Upload Invoice| CDN[CDN / Load Balancer]
    CDN -->|Serve App| Frontend[React Dashboard]
    
    subgraph "Frontend Layer"
        Frontend
    end

    %% Backend API
    subgraph "Backend API Layer (FastAPI)"
        API[FastAPI Server]
        Auth[Auth Middleware]
        Validation[Pydantic Validation]
        API --> Auth
        API --> Validation
    end
    
    Frontend -->|REST API Requests| API

    %% Async Task Queue
    subgraph "Asynchronous Processing (Celery)"
        Redis[(Redis Broker)]
        API -->|Enqueue Task| Redis
        
        WorkerOCR[Worker: Extraction & OCR]
        WorkerEmbed[Worker: Embedding & Vector]
        WorkerRisk[Worker: Risk Scoring]
        
        Redis -->|Consume| WorkerOCR
        Redis -->|Consume| WorkerEmbed
        Redis -->|Consume| WorkerRisk
    end

    %% AI Services
    subgraph "AI & Intelligence Layer"
        OpenAI_GPT[OpenAI GPT-4 (Extraction)]
        OpenAI_Ada[OpenAI text-embedding-ada-002]
        
        WorkerOCR -->|Structure Data| OpenAI_GPT
        WorkerEmbed -->|Generate Vectors| OpenAI_Ada
    end

    %% Data Storage
    subgraph "Data Persistence Layer"
        Postgres[(PostgreSQL\nRelational Data)]
        Qdrant[(Qdrant\nVector Database)]
        S3[Object Storage (S3)\nRaw Invoice Files]
        
        API -->|Read/Write Metadata| Postgres
        WorkerOCR -->|Update Invoice Data| Postgres
        WorkerRisk -->|Read/Write Scores| Postgres
        
        WorkerEmbed -->|Store/Search Vectors| Qdrant
        WorkerRisk -->|Similarity Search| Qdrant
        
        API -->|Upload File| S3
        WorkerOCR -->|Fetch File| S3
    end

    %% Notifications
    subgraph "Action & Notification Layer"
        Email[Email Service (SMTP/SES)]
        Slack[Slack Webhook]
        
        WorkerRisk -->|Alert High Risk| Email
        WorkerRisk -->|Notify Team| Slack
    end

    %% Flow connections
    WorkerOCR -->|Trigger Embedding| Redis
    WorkerEmbed -->|Trigger Scoring| Redis
```

## Component Breakdown

### 1. Frontend Layer (React)
- **Role**: User interface for uploading invoices, viewing dashboards, and manually reviewing flagged invoices.
- **Key Features**:
  - Drag-and-drop infoice upload.
  - Interactive dashboard for risk scores.
  - Side-by-side view of PDF and extracted data for auditing.

### 2. Backend API Layer (FastAPI)
- **Role**: Central entry point for all application logic.
- **Key Features**:
  - `POST /upload`: Handles file upload to S3 and creates a database record.
  - `GET /invoices`: specific invoice details.
  - **Pydantic Models**: Strictly validates data ingress/egress.
  - **Auth**: JWT-based authentication for secure access.

### 3. Asynchronous Processing (Celery + Redis)
- **Role**: Handles long-running and computational heavy tasks to keep the API responsive.
- **Workers**:
  - **Extraction Worker**: Orchestrates OCR (if needed) and LLM extraction.
  - **Embedding Worker**: ongoing vector generation for similarity search. 
  - **Risk Scoring Worker**: The core decision engine that calculates the 0-100 score based on multiple signals.

### 4. AI & Intelligence Layer
- **OpenAI GPT-4**: Used for "Structure Extraction". It takes raw text/images and forces them into the strict JSON schema defined by Pydantic.
- **OpenAI Ada-002**: Converts invoice text into vector embeddings to find semantic similarities (duplicates/fraud).

### 5. Data Persistence Layer
- **PostgreSQL**: Stores relational data (Vendors, Invoices, Users, Audit Logs).
- **Qdrant**: Stores vector embeddings. Optimized for cosine similarity search to find "near duplicate" invoices.
- **S3 / Object Storage**: Stores the actual PDF/Image files.

### 6. Action & Notification Layer
- Automated workflows based on risk score:
  - **Green (0-30)**: Auto-approve in DB.
  - **Yellow (31-60)**: Trigger email for manual review.
  - **Red (61-100)**: Block and alert via Slack/Email immediate.
