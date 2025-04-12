# Use the official AWS Lambda Python 3.9 base image
FROM public.ecr.aws/lambda/python:3.9

# Copy requirements.txt into the container
COPY requirements.txt ./

# Install required Python packages into the /var/task directory
# (the Lambda runtime looks for dependencies in /var/task by default)
RUN python3.9 -m pip install -r requirements.txt -t /var/task

# Copy your Lambda function code into /var/task
COPY lambda_function.py /var/task/

# Command to run your Lambda function by default
CMD [ "lambda_function.lambda_handler" ]