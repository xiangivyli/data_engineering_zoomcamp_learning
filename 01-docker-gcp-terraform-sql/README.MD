# the internmediate code when learning
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

# running postgres image locally with Docker
winpty docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /d/Learn_DS/Git/data_engineering_zoomcamp_learning/01-docker-gcp-terraform-sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13


# using `pgcli` for connecting to the database
winpty pgcli -h localhost -p 5432 -u root -d ny_taxi


# the pgadmin tool
winpty docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4


# two containers under the same Network
# create a network
docker network create pg-network

# run postgres image under the pg-network with a container named pg-database
winpty docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /d/Learn_DS/Git/data_engineering_zoomcamp_learning/01-docker-gcp-terraform-sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
postgres:13

winpty docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4

# use python script to get data, change data type and import postgresql
# use the green taxi trips from September 2019

# using Dockfile to build a complete image for the whole process
winpty docker build -t taxi_ingest:v001 .

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz"
winpty docker run -it \
    --network=01-docker-gcp-terraform-sql_default \
    --name ingest_data_2019_09 \
    taxi_ingest:v001 \
      --user=root \
      --password=root \
      --host=pgdatabase \
      --port=5432 \
      --db=ny_taxi \
      --table_name=green_taxi_data_2019_09 \
      --url=$URL

Local_URL="http://172.30.16.1:8000/green_taxi_data_2019_09.csv.gz"

URL2="https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"