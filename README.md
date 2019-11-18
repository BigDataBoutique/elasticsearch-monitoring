# Elasticsearch monitoring with Grafana

This repository contains everything required for end-to-end thorough monitoring of an Elasticsearch cluster.

Elasticsearch Monitoring was crafted and is continually being updated and improved based on experience with debugging and stabilizing many Elasticsearch clusters world-wide.

<img width="1237" alt="Elasticsearch monitoring with Grafana" src="https://user-images.githubusercontent.com/212252/27615380-63111928-5bb0-11e7-8857-48f041950f3f.png">

## Gathering metrics

### Using X-Pack Monitoring

Elastic's X-Pack Monitoring is provided with an agent that is shipping metrics to the cluster used for monitoring. This is a push-based approach, and requires installing the X-Pack plugin to your cluster. To go that route, please follow the installation instructions here: https://www.elastic.co/guide/en/x-pack/current/installing-xpack.html

### Using provided script

Another approach is to use the elasticsearch.monitoring script provided with this repository, which you can find at `elasticsearch.monitoring/fetch_stats.py`.
Use environment variables or program arguments to set the URLs for the monitored cluster and the cluster that is being used for monitoring.
By default they are both configured to be http://localhost:9200/ , make sure to use the format http://host:port/ .

* ES_METRICS_CLUSTER_URL - A comma seperated list of the monitored nodes (client/LB or data nodes if no other option).
* ES_METRICS_MONITORING_CLUSTER_URL - host for the monitoring cluster.
* ES_METRICS_INDEX_NAME - The index into which the monitoring data will go, can be left as is. default is '.monitoring-es-7-'

You can also set polling interval (10 seconds by default) and a prefix for the index name. See the script for more details.

Don't forget to install all dependencies by running:

`pip install -r requirements.txt`

The benefit of this approach is that it doesn't require installing a plugin, and is shipping the same bits (and even more) than the X-Pack Monitoring agent.

Once installed and configured, have the Python script run as a service to continuously collect metrics (with systemd for instance: https://linuxconfig.org/how-to-create-systemd-service-unit-in-linux).
At launch, you can see a printed message verifying the script is drawing data from and into the correct hosts.
To further validate that, you can also check the values of field source_node.host in the index with the monitoring data.

*NOTE:* If the cluster you are using for monitoring is 2.x, you will need to edit the template files (`elasticsearch.monitoring/templates/*`) and change all occurences of `"type": "keyword"` with `"type": "string", "index": "not_analyzed"`.

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
