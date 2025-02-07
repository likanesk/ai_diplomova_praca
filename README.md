<h1 align="left">Description 
  <img src="api/images/fastapi-logo.png" alt="FastAPI Logo" align="right" width="100"/>
</h1>

**Diplomova praca - Adrian Ihring**

This project includes a **backend** and a **frontend**:

- **Backend:**
  - **API:** Built using the FastAPI framework (Python).
  - **MinIO server:** For file management.
- **Frontend:**
  - **Next.js:** React-based framework.

---

## Locally Setup

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites
#### Development - locally
- Python 3.8 or newer
- Pip and virtualenv
- Git (for cloning the repository)
- MinIO server
- NodeJS
- NPM

### Installation
1. **Clone the Repository**

    ```bash
    $ git clone https://github.com/likanesk/ai_diplomova_praca.git
    ```

2. **Navigate to Backend Repository**

    ```bash
    $ cd ai_diplomova_praca/backend
    ```

2. **Install Dependencies**

    ```bash
    pip install -r requirements.txt
    ```

3. **Environment Configuration**

    Create a `.env` file based on the `.env.example` provided in the repository. Fill in the necessary environment variables such as database URLs, API keys, and other configurations.

    ```bash
    # Example
    MINIO_ENDPOINT=localhost:9000
    MINIO_ACCESS_KEY=APM5ncJe74pTaKorDxGM
    MINIO_SECRET_KEY=Sy5LTUUE5MdqC8CuYzZaRAgjdtA7aN4xtTkJViBc
    MINIO_SECURE=False
    ```

    These settings configure the application to connect to your local MinIO server. Adjust them according to your MinIO server setup if different from the above.
---

## Locally Startup

### Start MinIO server locally

1. **Navigate to Minio repository**

    ```bash
    # Example
    C:\Minio>
    ```

2. **Run minIO server**

    ```bash
    # Example
    $ minio.exe server C:\Minio\data
    ```

- **MinIO:** running at http://localhost:9000.

### Start Backend - FastAPI
1. **Navigate to Backend Repository**
    ```bash
    $ cd .\backend\
    ```

2. **Run Backend - API**
    ```bash
    $ fastapi dev main.py
    ```

- **API:** running at http://localhost:8000.

- **API documentation:** is available at http://127.0.0.1:8000/docs.
---

### Start Frontend - NextJS
1. **Navigate to Frontend Repository**
    ```bash
    $ cd .\frontend\
    ```

1. **Install packages**

    ```bash
    $ npm install
    ```

1. **Run NextJS Application**

    ```bash
    $ npm run dev
    ```

- **Frontend:** running at http://localhost:3000.

## Docker Setup for Production

This section provides instructions for running the project in a production environment using Docker. The setup includes MinIO, FastAPI (backend), and Next.js (frontend).

### Prerequisites
- Docker installed on your system.
- (Optional) Custom Docker network for inter-container communication (handled by Kubernetes in CI/CD).

---

### Step 0: Network Creation (Optional)
If you need a custom Docker network for inter-container communication, create it using the following command. This step is typically handled by Kubernetes in CI/CD pipelines and is not required unless you are running the containers locally.

```bash
$ docker network create my_network
```

---

### Step 1: Backend Setup (MinIO + FastAPI)
1. **Run Docker Container with MinIO**
- MinIO is used for file management. You can run it with or without a custom network.

    a. **Production WITHOUT custom network:**
    ```bash
    $ docker run -d -p 9000:9000 -p 9001:9001 --name minio \
    -e "MINIO_ACCESS_KEY=admin" \
    -e "MINIO_SECRET_KEY=password" \
    minio/minio server /data --console-address ":9001"
    ```

    b. **Production WITH custom network:**
    ```bash
    $ docker run -d -p 9000:9000 -p 9001:9001 --name minio --network my_network \
    -e "MINIO_ACCESS_KEY=admin" \
    -e "MINIO_SECRET_KEY=password" \
    minio/minio server /data --console-address ":9001"
    ```

2. **Build Docker Image for FastAPI (Backend)**
- Build the Docker image for the FastAPI backend.

    ```bash
    $ docker build -t ai-dp-rest-api .
    ```

3. **Run Docker Container with FastAPI (Backend)**
- Run the FastAPI backend container with or without a custom network.

    a. **Production WITHOUT custom network:**
    ```bash
    $ docker run -d -p 8000:8000 --name ai-dp-rest-api ai-dp-rest-api
    ```

    b. **Production WITH custom network:**
    ```bash
    $ docker run -d -p 8000:8000 --name ai-dp-rest-api --network my_network ai-dp-rest-api
    ```
---

### Step 2: Frontend Setup (Next.js)
1. **Build Docker Image for Next.js (Frontend)**
- Build the Docker image for the Next.js frontend.

    ```bash
    $ docker build -t ai-dp-nextjs .
    ```

2. **Run Docker Container with Next.js (Frontend)**
- Run the Next.js frontend container with or without a custom network.

    a. **Production WITHOUT custom network:**
    ```bash
    $ docker run -p 3000:3000 --name ai-dp-nextjs ai-dp-nextjs
    ```

    b. **Production WITH custom network:**
    ```bash
    $ docker run --network my_network -p 3000:3000 --name ai-dp-nextjs ai-dp-nextjs
    ```
---

## Additional Notes ##
- Ensure that the ports used in the Docker commands (e.g., 9000, 9001, 8000, 3000) are available on your system.

- Replace the environment variables (e.g., MINIO_ACCESS_KEY, MINIO_SECRET_KEY) with your own secure values in production.

- If you are using Kubernetes (K8s) in your CI/CD pipeline, the network setup will be managed by K8s, and you can skip the custom network creation step.
---

## License ##
This project is licensed under the [MIT License](LICENSE).

