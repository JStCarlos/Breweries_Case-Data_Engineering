# Dockerfile para Airflow com Python 3.10
FROM apache/airflow:2.10.4


USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
RUN export JAVA_HOME

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# # Defina o diretório de trabalho
# WORKDIR /opt/airflow

# # Copie as DAGs, configurações e outros arquivos necessários
# COPY dags/ /opt/airflow/dags/
# COPY include/ /opt/airflow/include/
# COPY config/ /opt/airflow/config/

# # Copie o arquivo de requisitos, se houver
# # COPY requirements.txt /opt/airflow/requirements.txt

# # Instale dependências adicionais
# # RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# # Defina variáveis de ambiente padrão
# ENV AIRFLOW_HOME=/opt/airflow
# ENV PYTHONPATH=/opt/airflow

# # Exponha a porta padrão do Airflow Webserver
# EXPOSE 8080

# # Comando padrão para o entrypoint (será sobrescrito pelo docker-compose)
# ENTRYPOINT ["airflow"]
