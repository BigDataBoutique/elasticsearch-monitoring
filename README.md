# Elasticsearch monitoring with Grafana

This repository contains everything required for end-to-end thorough monitoring of an Elasticsearch cluster.

Elasticsearch Monitoring was crafted and is continually being updated and improved based on experience with debugging and stabilizing many Elasticsearch clusters world-wide.

<img width="1237" alt="Elasticsearch monitoring with Grafana" src="https://user-images.githubusercontent.com/212252/27615380-63111928-5bb0-11e7-8857-48f041950f3f.png">

## Gathering metrics

### Using X-Pack Monitoring

Elastic's X-Pack Monitoring is provided with an agent that is shipping metrics to the cluster used for monitoring. This is a push-based approach, and requires installing the X-Pack plugin to your cluster. To go that route, please follow the installation instructions here: https://www.elastic.co/guide/en/x-pack/current/installing-xpack.html

### Using provided script

Another approach is to use the elasticsearch.monitoring script provided with this repository. Edit the parameters in `elasticsearch.monitoring/fetch_stats.py` file to set the URLs for the monitored cluster and the cluster that is being used for monitoring (by default they are configured to be the same).

Don't forget to install all dependencies by running:

`pip install -r requirements.txt`

The benefit of this approach is that it doesn't require installing a plugin, and is shipping the same bits (and even more) than the X-Pack Monitoring agent.

Once installed and configured, have the Python script run as a service to continuously collect metrics.

*NOTE:* If the cluster you are using for monitoring is 2.x, you will need to edit the template files (`elasticsearch.monitoring/templates/*`) and change all occurences of `"type": "keyword"` with `"type": "string", "index": "not_analyzed"`.

## Recommended settings

Default interval will poll Elasticsearch for metrics every 10 seconds.

## Visualizing with Grafana

In this repository there is also a Grafana dashboard you can import - `Elasticsearch Cluster Monitoring.json`.

You will need to create an Elasticsearch data source that is pointing to the cluster you use for monitoring, and set the index pattern to be `[.monitoring-es-2-]YYYY.MM.DD` and set Pattern to daily. The timestamp field is called `timestamp`. For more details see the official documentation here: http://docs.grafana.org/features/datasources/elasticsearch/.

For guidance on importing the dashboard see the official Grafana documentation: http://docs.grafana.org/reference/export_import/#importing-a-dashboard.

## Development and testing

```bash
docker run -p 9200:9200 -e "http.host=0.0.0.0" -e "transport.host=127.0.0.1" -e "xpack.security.enabled=false" -e "discovery.zen.minimum_master_nodes=1" docker.elastic.co/elasticsearch/elasticsearch:5.5.0
```
