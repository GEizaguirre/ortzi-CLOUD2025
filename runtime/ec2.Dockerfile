
# Python 3.10
FROM python:3.10-slim-buster

RUN apt-get update \
    # Install aws-lambda-cpp build dependencies
    && apt-get install -y \
      g++ \
      make \
      cmake \
      unzip \
      wget \
      zip \
    # cleanup package lists, they are not used anymore in this image
    && rm -rf /var/lib/apt/lists/* \
    && apt-cache search linux-headers-generic

ARG FUNCTION_DIR="/function"

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}

# Update pip
RUN pip install --upgrade --ignore-installed pip wheel six setuptools \
    && pip install --upgrade --no-cache-dir --ignore-installed \
        awslambdaric \
        boto3 \
        httplib2 \
        requests

ARG LITHOPS_VERSION

RUN pip install lithops==$LITHOPS_VERSION
RUN pip install lithops[aws]==$LITHOPS_VERSION

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

COPY requirements.txt ${FUNCTION_DIR}
RUN pip install -r ${FUNCTION_DIR}/requirements.txt
        
COPY ortzi.zip ${FUNCTION_DIR}
RUN mkdir ortzi \
    && mv ortzi.zip ortzi \
    && cd ortzi \
    && unzip ortzi.zip \
    && pip3 install -e . \
    && cd ..





