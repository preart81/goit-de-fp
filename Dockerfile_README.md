# Збірка Docker 

⚠️ Команди виконуються з під папки де файли!!!

1. Білдимо новий докер:
   ```sh
   docker build . -f Dockerfile --pull --tag my-image:0.0.1
   ```

2. Інитимо піднімаемо Докер компос:
   ```sh
   docker compose up airflow-init
   docker compose up
   ```