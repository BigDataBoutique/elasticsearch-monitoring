ARG ES_VERSION
FROM docker.elastic.co/elasticsearch/elasticsearch:$ES_VERSION
EXPOSE 9200
ARG ES_VERSION
ADD elasticsearch.yml /usr/share/elasticsearch/config/
USER root
RUN chown elasticsearch:elasticsearch config/elasticsearch.yml
USER elasticsearch
#WORKDIR /usr/share/elasticsearch
