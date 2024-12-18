# Dockerfile para Airflow com Python 3.10
FROM apache/airflow:2.8.1-python3.10

# Defina o diretório de trabalho
WORKDIR /opt/airflow

# Copie as DAGs, configurações e outros arquivos necessários
COPY dags/ /opt/airflow/dags/
COPY include/ /opt/airflow/include/
COPY config/ /opt/airflow/config/

# Copie o arquivo de requisitos, se houver
# COPY requirements.txt /opt/airflow/requirements.txt

# Instale dependências adicionais
# RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Defina variáveis de ambiente padrão
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow

# Exponha a porta padrão do Airflow Webserver
EXPOSE 8080

# Comando padrão para o entrypoint (será sobrescrito pelo docker-compose)
ENTRYPOINT ["airflow"]
