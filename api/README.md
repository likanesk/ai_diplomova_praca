<h1 align="left">API for Medical Data 
  <img src="images/fastapi-logo.png" alt="FastAPI Logo" align="right" width="100"/>
</h1>

This project is a FastAPI application designed to manage and process medical data securely. It ensures compliance with data protection regulations and provides a robust backend for medical data handling.

## Features

- **Secure Data Handling**: Implements best practices for data security and privacy.
- **API Access to Medical Data**: Facilitates CRUD operations on medical datasets.
- **Documentation and Testing**: Includes automated swagger documentation and a suite of unit tests.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Python 3.8 or newer
- Pip and virtualenv
- Git (for cloning the repository)
- MinIO server

### Installation

1. **Clone the Repository**

    ```bash
    git clone https://github.com/likanesk/ai_diplomova_praca.git
    cd ai_diplomova_praca/api
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

### Start minIO server

1. **Open cmd**

2. **Navigate to Minio repository**

    ```bash
    # Example
    C:\Users>cd ..
    C:\>cd Minio
    C:\Minio>
    ```

3. **Run minIO server**

    ```bash
    # Example
    C:\Minio>minio.exe server C:\Minio\data
    ```

### Running the Application

1. **Start the FastAPI Application**

    ```bash
    fastapi dev main.py
    ```

    This command will start the server on `http://127.0.0.1:8000`. The API documentation will be available at `http://127.0.0.1:8000/docs`.

## Acknowledgments

- FastAPI Team for the awesome framework.
- Python community for the continuous support.

