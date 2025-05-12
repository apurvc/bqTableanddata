# --------- stage 1: builder ---------
FROM python:3.12.9-slim-bullseye@sha256:44d4cea0ff1d49f79a5f2b4249cdcb40369e7d4ca8db83dd74bf13c56741a895 as builder

#set the working directory
WORKDIR /app

#install the required packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    git=1:2.30.2-1+deb11u4 \
    openssh-client=1:8.4p1-5+deb11u4 \
    software-properties-common=0.96.20.2-2.1 \
    build-essential=12.9 \
    ca-certificates=20210119 \
    libpq-dev=13.20-0+deb11u1 \
    apt-transport-https=2.2.4 \
    gnupg=2.2.27-2+deb11u2 \
    curl=7.74.0-1.3+deb11u14 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
          
#copy the requirements file
COPY requirements.txt .

#install the python packages
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

# --------- stage 2: final ---------
FROM python:3.12.9-slim-bullseye@sha256:44d4cea0ff1d49f79a5f2b4249cdcb40369e7d4ca8db83dd74bf13c56741a895

#set the working directory for the final image
WORKDIR /app

#copy the installed python packages from the builder image
COPY --from=builder /install /usr/local 

#copy the application code
COPY . .

#create a non-root user
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser /app

#switch to the non-root user
USER appuser

#run the application
CMD ["python", "migrate.py"]
