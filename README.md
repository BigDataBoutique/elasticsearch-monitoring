# Elasticsearch monitoring with Pulse

This repository contains everything required for end-to-end thorough monitoring of an Elasticsearch cluster.

In general, you should use metricbeat with Pulse in order to get the optimal monitoring experience for your Elasticsearch cluster.

Pulse was crafted and is continually being updated and improved based on experience with debugging and stabilizing many Elasticsearch clusters world-wide.

For more information on Pulse:

https://bigdataboutique.com/contact

As metricbeat is not an option for AWS Elasticsearch clusters, you can use this script to fetch data from them.

This repository still allows relaying monitoring data to any Elasticsearch target, and contains a Grafana dashboard that works with the data created by it.

However further improvements as well as advanced capabilities such as alerts are available only for Pulse.


<img width="1237" alt="Elasticsearch monitoring with Grafana" src="https://gitlab.com/BigDataBoutique/elasticsearch-monitoring/uploads/1ce902cde681991af6d0bd51c2c606f1/pulse.jpg">

## Gathering metrics

### Using Metricbeat

Elastic's Metricbeat is provided with an agent that can ship metrics to the cluster used for monitoring. This is a push-based approach, and requires installing and configuring metricbeat. To go that route, please follow the installation instructions here: https://www.elastic.co/guide/en/elasticsearch/reference/current/configuring-metricbeat.html

### Using provided script

Another approach is to use the elasticsearch.monitoring script provided with this repository, which you can find at `elasticsearch.monitoring/fetch_stats.py`.
You can either do this directly with python, or use the Dockerfile in this repository. See instructions for docker use below.
Use environment variables or program arguments to set the URLs for the monitored cluster and the cluster that is being used for monitoring.
By default they are both configured to be http://localhost:9200/ , make sure to use the format http://host:port/ .

* ES_METRICS_CLUSTER_URL - A comma seperated list of the monitored nodes (client/LB or data nodes if no other option).
* ES_METRICS_MONITORING_CLUSTER_URL - host for the monitoring cluster.
* ES_METRICS_INDEX_NAME - The index into which the monitoring data will go, can be left as is. default is '.monitoring-es-7-'
* NUMBER_OF_REPLICAS - number of replicas for indices created on the monitoring server. Default is 1
* ES_METRICS_MONITORING_AUTH_TOKEN - X-Auth-Token header value if required for the target cluster
* ES_METRICS_CLOUD_PROVIDER - either 'Amazon Elasticsearch' or 'Elastic Cloud' or not provided

You can also set polling interval (10 seconds by default) and a prefix for the index name. See the script for more details.

Don't forget to install all dependencies by running:

`pip install -r requirements.txt`

The benefit of this approach is that it doesn't require installing a plugin, and is shipping the necessary information, with some important extras, as opposed to the Metricbeat approach.

Once installed and configured, have the Python script run as a service to continuously collect metrics (with systemd for instance: https://linuxconfig.org/how-to-create-systemd-service-unit-in-linux).
At launch, you can see a printed message verifying the script is drawing data from and into the correct hosts.
To further validate that, you can also check the values of field source_node.host in the index with the monitoring data.

*NOTE:* If the cluster you are using for monitoring is 2.x, you will need to edit the template files (`elasticsearch.monitoring/templates/*`) and change all occurences of `"type": "keyword"` with `"type": "string", "index": "not_analyzed"`.

### Docker setup

`sudo apt update`

`sudo apt install docker.io`

`sudo docker build . -t fetch_stats`

`sudo docker run --net=host --env ES_METRICS_CLUSTER_URL=http://localhost:9200/ fetch_stats /app/elasticsearch.monitoring/fetch_stats.py`

where ES_METRICS_CLUSTER_URL is setup to the monitored ES, and obviously adding additional variables if required.
Run once in the foreground to validate that the script works correctly, then use -d in the docker run to run in background.

### AWS pulse setup

Same as above - you'll get specific values for ES_METRICS_MONITORING_CLUSTER_URL and ES_METRICS_MONITORING_AUTH_TOKEN,

In addition you should set ES_METRICS_CLOUD_PROVIDER to 'Amazon Elasticsearch'.

when running, make sure to set --health-flag=False . 

## Visualizing with Grafana

You will need to create an Elasticsearch data source that is pointing to the cluster you use for monitoring.
The following are the settings that conform with the data collected by the provided script:
* Assuming Grafana runs from the same host as the monitor, leave URL as `http://localhost:9200`, otherwise change it
* Set the name to `Elasticsearch Monitor`
* Set the index pattern to be `[.monitoring-es-*-]YYYY.MM.DD`
* Set the version to the version of your monitoring cluster
* Set Pattern to daily.
* Set the timestamp field to `timestamp`.
* Save and test the connection.
You can now import the dashboard found at `Elasticsearch Cluster Monitoring.json` and start monitoring your cluster!

For more details see the official documentation here: http://docs.grafana.org/features/datasources/elasticsearch/.

For guidance on importing the dashboard see the official Grafana documentation: http://docs.grafana.org/reference/export_import/#importing-a-dashboard.

An outdated, but useful, information on collected metrics:
https://www.elastic.co/guide/en/elasticsearch/guide/current/_monitoring_individual_nodes.html
