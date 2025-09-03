requirements: docker & docker composer

create the .env file based on the .env.example file

docker compose build

docker compose up airflow-init

docker compose up

ctrl+c to stop the service

docker compose down

enter http://localhost:8080/ on the web browser to have access to the airflow interface

create a python virtual environment and install the python libraries based on the atividade5/requirements.txt file