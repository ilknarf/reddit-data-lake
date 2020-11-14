FROM python:3.8-alpine

LABEL author="ilknarf"

COPY . /app
WORKDIR /app

# install gcc
RUN apk add --no-cache gcc musl-dev
RUN pip install -r requirements.txt

ENTRYPOINT [ "python", "main.py" ]
CMD [ "all" ]