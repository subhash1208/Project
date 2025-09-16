FROM 683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:0.90-1-cpu-py3
# COPY ml_pipeline/. /module

# WORKDIR /

# Set some environment variables. PYTHONUNBUFFERED keeps Python from buffering our standard
# output stream, which means that logs can be delivered to the user quickly. PYTHONDONTWRITEBYTECODE
# keeps Python from writing the .pyc files which are unnecessary in this case. We also update
# PATH so that the train and serve programs are found when the container is invoked.

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE

# Set up the program in the image
COPY code /opt/ml/code/
WORKDIR /opt/ml/code/
RUN pip3 install --trusted-host artifactory.us.caas.oneadp.com -i https://artifactory.us.caas.oneadp.com/artifactory/api/pypi/pypi/simple/ -r requirements.txt

RUN chmod +x /opt/ml/code/*

EXPOSE 8080

#CMD ["python", "model_inference.py"]
