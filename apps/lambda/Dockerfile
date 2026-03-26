FROM public.ecr.aws/lambda/python:3.11

# Copy the entire project for package installation
COPY . ${LAMBDA_TASK_ROOT}

# Install dependencies and the local cleanarr package
RUN pip install --no-cache-dir .

# Set the handler
CMD ["apps.lambda.main.lambda_handler"]
