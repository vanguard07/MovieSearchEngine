FROM node:18-alpine AS build-frontend
WORKDIR /frontend
COPY frontend/package.json .
RUN npm install
COPY frontend/ ./
RUN npm run build


FROM spark:3.5.0-python3
USER root
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip install -r requirements.txt

# Install dependencies from requirements.txt
COPY --from=build-frontend /frontend/build/ /app/build
COPY . /app/

RUN chmod +x script.sh

# Run the script with spark-submit
CMD ["/app/script.sh"]