FROM ubuntu:latest
 
# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    git \
    curl \
    wget \
    unzip \
    libglib2.0-dev \
    libgl1 libgl1-mesa-dri \
&& rm -rf /var/lib/apt/lists/*
 
# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
&& unzip awscliv2.zip \
&& ./aws/install \
&& rm -rf awscliv2.zip aws
 
#copy the scripte to the docker
COPY scripts/* /home/ubuntu/

# RUN git clone https://github.com/ultralytics/yolov5.
RUN mkdir -p  /home/ubuntu/image_detection
# Create and activate virtual environment
RUN python3 -m venv /home/ubuntu/image_detection
#ENV PATH="/opt/venv/bin:$PATH"
RUN /bin/bash -c "source /home/ubuntu/image_detection/bin/activate && pip install boto3 ultralytics flask gitpython"