#!/usr/bin/env python
import json
import os
import datetime
import socket
import time
import sys
import random
import logging
import numbers, types

from IndexStatsDeltaMapper import IndexStatsDeltaMapper
from NodeStatsDeltaMapper import NodeStatsDeltaMapper

import click
import requests

working_dir = os.path.dirname(os.path.realpath(__file__))
r_json_node_prev = {}
r_json_index_prev = {}
clusters_dictionary = {
    "b1780b614f5d4e0b8e3ad7e823889a8e": "Y2-EC-Prod",
    "00df2ff0bd8944e6868ba73f991f9a1d": "Y2-EC-Dev",
    "f557cc5f740b448785b82e7539490bed": "Y2-EC-Monitor",
    "55c9d320289c47eb80f485c0f53bbc5f": "Y2-EC-Logs",
    "4fb043c1b0ef4c3ba5fb94e8e74e5657": "Y2-EC-Yzer"
}

def merge(one, two):
    cp = one.copy()
    cp.update(two)
    return cp

def color_to_level(color):
    return {
        'green': 0,
        'yellow': 1,
        'red': 2
    }.get(color, 3)

def lookup(data, selector):
    keys = selector.split('.')
    value = data
    while keys:
        value = value[keys.pop(0)]
    return value

def delete_path(data, selector):
    keys = selector.split('.')
    value = data
    while keys:
        k = keys.pop(0)
        if k not in value:
            return
        value = value[k]

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(y)
    return out

def assert_http_status(response, expected_status_code=200):
    if response.status_code != expected_status_code:
        print(response.url, response.json())
        raise Exception('Expected HTTP status code %d but got %d' % (expected_status_code, response.status_code))

cluster_uuid = None
cluster_name = None

# Elasticsearch Cluster to Send metrics to
monitoringCluster = os.environ.get('ES_METRICS_MONITORING_CLUSTER_URL', 'http://localhost:9200/')
if not monitoringCluster.endswith("/"):
    monitoringCluster = monitoringCluster + '/'
indexPrefix = os.environ.get('ES_METRICS_INDEX_NAME', '.monitoring-es-7-')
numberOfReplicas = os.environ.get('NUMBER_OF_REPLICAS', '1')

def fetch_cluster_health(base_url='http://localhost:9200/'):
    utc_datetime = datetime.datetime.utcnow()
    try:
        response = requests.get(base_url + '_cluster/health', timeout=(5, 5))
        jsonData = response.json()
        jsonData['timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
        jsonData['status_code'] = color_to_level(jsonData['status'])
        return [jsonData]
    except (requests.exceptions.Timeout, socket.timeout):
        print("[%s] Timeout received on trying to get cluster health" % (time.strftime("%Y-%m-%d %H:%M:%S")))
        return []

node_stats_to_collect = ["indices", "os", "process", "jvm", "thread_pool", "fs", "transport", "http", "breakers", "script"]
def fetch_nodes_stats(base_url='http://localhost:9200/'):
    metric_docs = []
    global r_json_node_prev
    try:
        response = requests.get(base_url + '_nodes/stats', timeout=(5, 5))
        r_json = response.json()
        cluster_name = r_json['cluster_name']
        if cluster_name in clusters_dictionary:
            cluster_name = clusters_dictionary[r_json['cluster_name']]


        # we are opting to not use the timestamp as reported by the actual node
        # to be able to better sync the various metrics collected
        utc_datetime = datetime.datetime.utcnow()

        for node_id, node in r_json['nodes'].items():
            print("Building log for node " + node['name'])
            node_data = {
                "timestamp": str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'),
                "cluster_name": cluster_name,
                "cluster_uuid": cluster_uuid,
                "source_node": {
                    "uuid": node_id,
                    "host": node['host'],
                    "transport_address": node['transport_address'],
                    "ip": node['ip'],
                    "name": node['name'],
                    "roles": node.get('roles'),
                    "attributes": {} # TODO do we want to bring anything here?
                },
            }
            is_master = ('roles' in node and 'master' in node['roles']) or ('attributes' in node and 'master' in node['attributes'] and (
                        node['attributes']['master'] == 'true' or node['attributes']['master'] == True))
            node_data["node_stats"] = {
                "node_id": node_id,
                "node_master": is_master,
                "mlockall": True, # TODO here for compat reasons only
            }

            for k in node_stats_to_collect:
                node_data["node_stats"][k] = node.get(k)

            # clean up some stuff
            delete_path(node_data["node_stats"], "os.timestamp")
            del node_data["node_stats"]["process"]["timestamp"]
            del node_data["node_stats"]["os"]["timestamp"]
            del node_data["node_stats"]["jvm"]["timestamp"]
            del node_data["node_stats"]["jvm"]["mem"]["pools"]
            del node_data["node_stats"]["jvm"]["buffer_pools"]
            del node_data["node_stats"]["jvm"]["uptime_in_millis"]
            # TODO remove some thread pools stats
            del node_data["node_stats"]["fs"]["timestamp"]
            del node_data["node_stats"]["fs"]["data"]

            # shaig 4.2 - adding data for current and average query time
            if r_json_node_prev:
                try: 
                    prev_data = r_json_node_prev['nodes'][node_id]
                    calc_node_delta_statistics(node_data, prev_data)
                except KeyError:
                    node_data["node_stats"]["missing_previous"] = True
            metric_docs.append(node_data)
    except (requests.exceptions.Timeout, socket.timeout):
        print("[%s] Timeout received on trying to get cluster health" % (time.strftime("%Y-%m-%d %H:%M:%S")))

    # shaig 4.2 - adding data for current and average query time
    r_json_node_prev = r_json
    return metric_docs

def calc_node_delta_statistics(node_data, prev_data):
    mapper = NodeStatsDeltaMapper(node_data["node_stats"], prev_data)
    #Search statistics
    node_data["node_stats"]["indices"]["search"]["query_time_current"] = mapper.getQueryTime()
    node_data["node_stats"]["indices"]["search"]["query_count_delta"] = mapper.getQueryTotal()
    node_data["node_stats"]["indices"]["search"]["query_avg_time"] = mapper.getAvgQueryTime()
    node_data["node_stats"]["indices"]["search"]["fetch_time_current"] = mapper.getFetchTime()
    node_data["node_stats"]["indices"]["search"]["fetch_count_delta"] = mapper.getFetchTotal()
    node_data["node_stats"]["indices"]["search"]["query_then_fetch_avg_time"] = mapper.getAvgQueryThenFetchTime()

    # machine statistics
    node_data["node_stats"]["indices"]["merges"]["avg_size_in_bytes"] = mapper.getMergeTotal()
    node_data["node_stats"]["fs"]["io_stats"]["total"]["write_ops_current"] = mapper.getWriteOperations()
    node_data["node_stats"]["fs"]["io_stats"]["total"]["read_ops_current"] = mapper.getReadOperations()
    node_data["node_stats"]["fs"]["total"]["free_ratio"] =\
        node_data["node_stats"]["fs"]["total"]["free_in_bytes"] / node_data["node_stats"]["fs"]["total"]["total_in_bytes"]


# shaig 18.3 - adding data for index stats
def fetch_index_stats(base_url='http://localhost:9200/'):
    metric_docs = []
    global r_json_index_prev
    try:
        # getting timestamp
        utc_datetime = datetime.datetime.utcnow()

        # checking which indices are required
        response_indices = requests.get(base_url + 'indices_to_query/_search', timeout=(5, 5))
        print("Got indices_to_query" + base_url)
        if response_indices.status_code != 200:
            return None
        indices_json = response_indices.json()

        indices_hits = indices_json['hits']['hits']
        indices_to_query = ""
        for idx,d in enumerate(indices_hits):
            if idx != 0:
                indices_to_query += ','
            index_name = d['_source']['index_name']
            indices_to_query += index_name
        if indices_to_query is "":
            return None

        # getting index stats for all relevant indices
        response = requests.get(base_url + indices_to_query + '/_stats', timeout=(5, 5))
        if response.status_code != 200:
            return None
        r_json = response.json()

        # creating index stats json
        for index_name in r_json['indices']:
            print("Building log for index " + index_name)
            index_data = {
                "timestamp": str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'),
                "cluster_name": cluster_name,
                "cluster_uuid": cluster_uuid
                }
            index_data['index_stats'] = r_json['indices'][index_name]
            index_data['index_stats']['index'] = index_name

            if r_json_index_prev:
                try:
                    prev_data = r_json_index_prev['indices'][index_name]
                    calc_index_delta_statistics(index_data, prev_data)
                except KeyError:
                    index_data['index_stats']["missing_previous"] = True
            metric_docs.append(index_data)
    except (requests.exceptions.Timeout, socket.timeout):
        print("[%s] Timeout received on getting index stats" % (time.strftime("%Y-%m-%d %H:%M:%S")))

    r_json_index_prev = r_json
    return metric_docs


def calc_index_delta_statistics(index_data, prev_data):
    mapper = IndexStatsDeltaMapper(index_data['index_stats'], prev_data)
    index_data['index_stats']["primaries"]["search"]["query_time_current"] = mapper.getQueryTime()
    index_data['index_stats']["primaries"]["search"]["query_count_delta"] = mapper.getQueryTotal()
    index_data['index_stats']["primaries"]["search"]["query_avg_time"] = mapper.getAvgQueryTime()
    index_data['index_stats']["primaries"]["merges"]["avg_size_in_bytes"] = mapper.getMergeTotal()
    index_data['index_stats']["primaries"]["docs"]["count_delta"] = mapper.getCountDelta()
    index_data['index_stats']["primaries"]["docs"]["deleted_delta"] = mapper.getDeletedDelta()


def create_templates():
    for filename in os.listdir(os.path.join(working_dir, 'templates')):
        if filename.endswith(".json"):
            with open(os.path.join(working_dir, 'templates', filename)) as query_base:
                template = query_base.read()
                template = template.replace('{{INDEX_PREFIX}}', indexPrefix + '*').strip()
                template = template.replace('{{NUMBER_OF_REPLICAS}}', numberOfReplicas).strip()
                templates_response = requests.put(monitoringCluster + '_template/' + indexPrefix.replace('.', '') + filename[:-5],
                                                  data = template,
                                                  headers={'Content-Type': 'application/json;charset=UTF-8'},
                                                  timeout=(30, 30))
                assert_http_status(templates_response)


def poll_metrics(cluster_host, monitor, monitor_host):
    cluster_health, node_stats,index_stats = get_all_data(cluster_host)
    if monitor == 'elasticsearch':
        into_elasticsearch(monitor_host, cluster_health, node_stats,index_stats)
    elif monitor == 'signalfx':
        into_signalfx(monitor_host, cluster_health, node_stats)


def get_all_data(cluster_host):
    cluster_health = fetch_cluster_health(cluster_host)
    node_stats = fetch_nodes_stats(cluster_host)
    index_stats = fetch_index_stats(cluster_host)
    # TODO generate cluster_state documents
    return cluster_health, node_stats,index_stats


def into_signalfx(sfx_key, cluster_health, node_stats):
    import signalfx
    sfx = signalfx.SignalFx()
    ingest = sfx.ingest(sfx_key)
    for node in node_stats:
        source_node = node['source_node']
        for s in node_stats_to_collect:
            flattened = flatten_json(node['node_stats'][s])
            for k,v in flattened.items():
                if isinstance(v, (int, float)) and not isinstance(v, types.BooleanType):
                    ingest.send(gauges=[{"metric": 'elasticsearch.node.' + s + '.' + k, "value": v,
                                         "dimensions": {
                                             'cluster_uuid': node.get('cluster_uuid'),
                                             'cluster_name': node.get('cluster_name'),
                                             'node_name': source_node.get('name'),
                                             'node_host': source_node.get('host'),
                                             'node_host': source_node.get('ip'),
                                             'node_uuid': source_node.get('uuid'),
                                             'cluster_name': source_node.get('uuid'),
                                             }
                                         }])
    ingest.stop()

def into_elasticsearch(monitor_host, cluster_health, node_stats,index_stats):
    utc_datetime = datetime.datetime.utcnow()
    index_name = indexPrefix + str(utc_datetime.strftime('%Y.%m.%d'))

    cluster_health_data = ['{"index":{"_index":"'+index_name+'","_type":"_doc"}}\n' + json.dumps(with_type(o, 'cluster_health'))+'\n' for o in cluster_health]
    node_stats_data = ['{"index":{"_index":"'+index_name+'","_type":"_doc"}}\n' + json.dumps(with_type(o, 'node_stats'))+'\n' for o in node_stats]
    data = node_stats_data + cluster_health_data
    if index_stats is not None:
        index_stats_data = ['{"index":{"_index":"' + index_name + '","_type":"_doc"}}\n' + json.dumps(
            with_type(o, 'index_stats')) + '\n' for o in index_stats]
        data += index_stats_data

    try:
        bulk_response = requests.post(monitor_host + index_name + '/_bulk',
                                      data='\n'.join(data),
                                      headers={'Content-Type': 'application/x-ndjson'},
                                      timeout=(30, 30))
        assert_http_status(bulk_response)
        for item in bulk_response.json()["items"]:
            if item.get("index") and item.get("index").get("status") != 201:
                click.echo(json.dumps(item.get("index").get("error")))
    except (requests.exceptions.Timeout, socket.timeout):
        print("[%s] Timeout received while pushing collected metrics to Elasticsearch" % (time.strftime("%Y-%m-%d %H:%M:%S")))


def with_type(o, _type):
    o["type"] = _type
    return o


@click.command()
@click.option('--interval', default=10, help='Interval (in seconds) to run this')
@click.option('--index-prefix', default='', help='Index prefix for Elastic monitor')
@click.argument('monitor-host', default=monitoringCluster)
@click.argument('monitor', default='elasticsearch')
@click.argument('cluster-host', default='http://10.0.0.59:9200/')
def main(interval, cluster_host, monitor, monitor_host, index_prefix):
    global cluster_uuid, cluster_name, indexPrefix

    monitored_cluster = os.environ.get('ES_METRICS_CLUSTER_URL', cluster_host)
    if ',' in monitored_cluster:
        cluster_hosts = monitored_cluster.split(',')
    else:
        cluster_hosts = [monitored_cluster]

    indexPrefix = index_prefix or indexPrefix

    click.echo('[%s] Monitoring %s into %s at %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), monitored_cluster, monitor, monitor_host))

    init = False
    while not init:
        try:
            response = requests.get(random.choice(cluster_hosts), timeout=(5, 5))
            assert_http_status(response)
            cluster_uuid = response.json().get('cluster_uuid')
            cluster_name = response.json().get('cluster_name')
            init = True #
        except (requests.exceptions.Timeout, socket.timeout, requests.exceptions.ConnectionError):
            click.echo("[%s] Timeout received on trying to get cluster uuid" % (time.strftime("%Y-%m-%d %H:%M:%S")))
            sys.exit(1)

    click.echo('[%s] Started' % (time.strftime("%Y-%m-%d %H:%M:%S"),))

    recurring = interval > 0
    if not recurring:
        poll_metrics(cluster_host, monitor, monitor_host)
    else:
        try:
            nextRun = 0
            while True:
                if time.time() >= nextRun:
                    nextRun = time.time() + interval
                    now = time.time()

                    poll_metrics(random.choice(cluster_hosts), monitor, monitor_host)

                    elapsed = time.time() - now
                    click.echo("[%s] Total Elapsed Time: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), elapsed))
                timeDiff = nextRun - time.time()
                # Check timediff , if timediff >=0 sleep, if < 0 send metrics to es
                if timeDiff >= 0:
                    time.sleep(timeDiff)
        except requests.exceptions.ConnectionError as e:
            click.echo("[%s] FATAL Connection error %s, quitting" % (time.strftime("%Y-%m-%d %H:%M:%S"), e))
            sys.exit(1)
        except KeyboardInterrupt:
            click.echo('Interrupted')
            try:
                sys.exit(0)
            except SystemExit as e:
                print(e)
                os._exit(1)


if __name__ == '__main__':
    main()
