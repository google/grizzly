# Build step #1: build the React front end
FROM node:16-alpine as build-step
WORKDIR /app/frontend
ENV PATH /app/frontend/node_modules/.bin:$PATH
COPY frontend/package.json frontend/package-lock.json ./
COPY ./frontend/src ./src
COPY ./frontend/public ./public
RUN npm install
RUN npm run build

# Build step #2: build the API with the client as static files
FROM python:3.9
WORKDIR /app
COPY --from=build-step /app/frontend/build ./frontend/build

COPY requirements.txt ./
COPY ./sql_graph ./sql_graph
COPY ./backend ./backend
RUN pip install -r ./requirements.txt
ENV FLASK_ENV production
ENV GRIZZLY_DATA_LINEAGE_PROJECTS "dev,uat,prod"


EXPOSE 8080
WORKDIR /app/
CMD ["gunicorn", "-b", ":8080", "backend.app:app"]