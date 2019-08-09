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

import click
import requests

working_dir = os.path.dirname(os.path.realpath(__file__))

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

# Elasticsearch Cluster to Send metrics to
monitoringCluster = os.environ.get('ES_METRICS_MONITORING_CLUSTER_URL', 'http://localhost:9200/')
if not monitoringCluster.endswith("/"):
    monitoringCluster = monitoringCluster + '/'
indexPrefix = os.environ.get('ES_METRICS_INDEX_NAME', '.monitoring-es-7-')

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

node_stats_to_collect = ["indices", "os", "process", "jvm", "thread_pool", "fs", "transport", "http", "script"]
def fetch_nodes_stats(base_url='http://localhost:9200/'):
    metric_docs = []

    try:
        response = requests.get(base_url + '_nodes/stats', timeout=(5, 5))
        r_json = response.json()
        cluster_name = r_json['cluster_name']

        # we are opting to not use the timestamp as reported by the actual node
        # to be able to better sync the various metrics collected
        utc_datetime = datetime.datetime.utcnow()

        for node_id, node in r_json['nodes'].items():
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
                    "attributes": {} # TODO do we want to bring anything here?
                },
            }
            is_master = ('roles' in node and 'master' in node['roles']) or ('attributes' in node and (node['attributes']['master'] == 'true' or node['attributes']['master'] == True))
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

            metric_docs.append(node_data)
    except (requests.exceptions.Timeout, socket.timeout):
        print("[%s] Timeout received on trying to get cluster health" % (time.strftime("%Y-%m-%d %H:%M:%S")))

    return metric_docs

def fetch_index_stats():
    # TODO
    pass

def create_templates():
    for filename in os.listdir(os.path.join(working_dir, 'templates')):
        if filename.endswith(".json"):
            with open(os.path.join(working_dir, 'templates', filename)) as query_base:
                template = query_base.read()
                template = template.replace('{{INDEX_PREFIX}}', indexPrefix + '*').strip()
                templates_response = requests.put(monitoringCluster + '_template/' + filename[:-5],
                                                  data = template,
                                                  headers={'Content-Type': 'application/json;charset=UTF-8'},
                                                  timeout=(30, 30))
                assert_http_status(templates_response)


def poll_metrics(cluster_host, monitor, monitor_host):
    cluster_health, node_stats = get_all_data(cluster_host)
    if monitor == 'elasticsearch':
        into_elasticsearch(monitor_host, cluster_health, node_stats)
    elif monitor == 'signalfx':
        into_signalfx(monitor_host, cluster_health, node_stats)


def get_all_data(cluster_host):
    cluster_health = fetch_cluster_health(cluster_host)
    node_stats = fetch_nodes_stats(cluster_host)
    # TODO generate cluster_state documents
    return cluster_health, node_stats


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

def into_elasticsearch(monitor_host, cluster_health, node_stats):
    utc_datetime = datetime.datetime.utcnow()
    index_name = indexPrefix + str(utc_datetime.strftime('%Y.%m.%d'))

    cluster_health_data = ['{"index":{"_index":"'+index_name+'","_type":"_doc"}}\n' + json.dumps(with_type(o, 'cluster_health'))+'\n' for o in cluster_health]
    node_stats_data = ['{"index":{"_index":"'+index_name+'","_type":"_doc"}}\n' + json.dumps(with_type(o, 'node_stats'))+'\n' for o in node_stats]

    data = node_stats_data + cluster_health_data

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
@click.option('--index-prefix', default='.monitoring-es-2-', help='Index prefix for Elastic monitor')
@click.argument('monitor-host', default=monitoringCluster)
@click.argument('monitor', default='elasticsearch')
@click.argument('cluster-host', default='http://localhost:9200/')
def main(interval, cluster_host, monitor, monitor_host, index_prefix):
    global cluster_uuid, indexPrefix

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
            init = True
            if monitor == 'elasticsearch':
                try:
                    create_templates()
                except (requests.exceptions.Timeout, socket.timeout, requests.exceptions.ConnectionError):
                    click.echo("[%s] Timeout received when trying to put template" % (time.strftime("%Y-%m-%d %H:%M:%S")))
            elif monitor == 'signalfx':
                import signalfx
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
