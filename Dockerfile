# Use the jupyter/pyspark-notebook as the base image
FROM jupyter/pyspark-notebook:latest

USER root

# Configuring cron job
#RUN apt-get update && apt-get -y install cron
#COPY etl_cron /etc/cron.d/etl_cron
#RUN chmod 0644 /etc/cron.d/etl_cron
#RUN crontab /etc/cron.d/etl_cron
#RUN touch /var/log/cron.log
#CMD cron && tail -f /var/log/cron.log

# Download the PostgreSQL JDBC driver
ENV POST_URL https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN wget ${POST_URL}
RUN mv postgresql-42.2.5.jar /usr/local

# Switch back to the default jovyan user
USER $NB_UID

#Install the required Python packages
COPY requirements.txt /home/$NB_USER/requirements.txt
RUN pip3 install -r /home/$NB_USER/requirements.txt


