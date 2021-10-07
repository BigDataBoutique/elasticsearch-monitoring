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
import urllib3

logger = logging.getLogger(__name__)

working_dir = os.path.dirname(os.path.realpath(__file__))
r_json_node_prev = {}
# r_json_index_prev = {}
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
        logger.info(response.url, response.json())
        raise Exception('Expected HTTP status code %d but got %d' % (expected_status_code, response.status_code))

cluster_uuid = None

# Elasticsearch Cluster to Send metrics to
monitoringCluster = os.environ.get('ES_METRICS_MONITORING_CLUSTER_URL', 'http://localhost:9200/')
if not monitoringCluster.endswith("/"):
    monitoringCluster = monitoringCluster + '/'
indexPrefix = os.environ.get('ES_METRICS_INDEX_NAME', '.monitoring-es-7-')
numberOfReplicas = os.environ.get('NUMBER_OF_REPLICAS', '1')

#shaig 19.7 - turning cluster health into cluster stats
def fetch_cluster_stats(base_url='http://localhost:9200/',health = True,verify=True,cloud_provider = None):
    metric_docs = []
    try:
        response = requests.get(base_url + '_cluster/health', timeout=(5, 5),verify=verify)
        if response.status_code != 200:
            logger.error("bad response while getting cluster health. response is:\n" + response.json())
            return metric_docs,None
        cluster_health = response.json()
        utc_datetime = datetime.datetime.utcnow()
        ts = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
        cluster_health['timestamp'] = ts
        cluster_health['status_code'] = color_to_level(cluster_health['status'])
        if health:
            metric_docs.append(cluster_health)
        response = requests.get(base_url + '_cluster/stats', timeout=(5, 5),verify=verify)
        if response.status_code != 200:
            logger.error("bad response while getting cluster stats. response is:\n" + response.json())
            return metric_docs,None
        cluster_stats = response.json()
        # creating cluster stats json
        cluster_stats_and_state = {'type':'cluster_stats','cluster_stats': cluster_stats, 'cluster_name': cluster_stats['cluster_name'], 'timestamp': ts, '@timestamp': cluster_stats['timestamp'],'cluster_uuid':cluster_stats['cluster_uuid'],'cloud_provider':cloud_provider}
        response = requests.get(base_url + '_cluster/state', timeout=(5, 5),verify=verify)
        if response.status_code != 200:
            logger.error("bad response while getting cluster state. response is:\n" + response.json())
            return metric_docs,None
        cluster_state = response.json()
        cluster_state_json = {'nodes': cluster_state['nodes'],'cluster_uuid':cluster_state['cluster_uuid'],'state_uuid':cluster_state['state_uuid'],'master_node':cluster_state['master_node'],'version':cluster_state['version'],'status':cluster_health['status']}
        routing_table = cluster_state['routing_table']['indices']
        if type(routing_table) is not dict:
            logger.error("bad routing table from cluster state. response is:\n"+ response.json())
        cluster_stats_and_state['cluster_state'] = cluster_state_json
        metric_docs.append(cluster_stats_and_state)
        return metric_docs,routing_table
    except (requests.exceptions.Timeout, socket.timeout) as e:
        logger.error("[%s] Timeout received on trying to get cluster stats" % (time.strftime("%Y-%m-%d %H:%M:%S")))
        return [],[]

node_stats_to_collect = ["indices", "os", "process", "jvm", "thread_pool", "fs", "transport", "http", "breakers", "script"]

def fetch_nodes_stats(base_url='http://localhost:9200/',verify=True):
    metric_docs = []
    global r_json_node_prev
    r_json = None
    try:
        response = requests.get(base_url + '_nodes/stats', timeout=(5, 5),verify=verify)
        if response.status_code != 200:
            logger.error("bad response while getting node stats. response is:\n" + response.json())
            return metric_docs
        r_json = response.json()
        cluster_name = r_json['cluster_name']

        # we are opting to not use the timestamp as reported by the actual node
        # to be able to better sync the various metrics collected
        utc_datetime = datetime.datetime.utcnow()
        ts = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
        nodes_dict = r_json['nodes']
        if type(nodes_dict) is not dict:
            logger.error("bad node stats. response is:\n" + response.json())
            return metric_docs
        for node_id, node in nodes_dict.items():
            doc_timestamp = datetime.datetime.fromtimestamp(node['timestamp']/1000.0)
            doc_ts = str(doc_timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
            node_data = {
                "timestamp": ts,
                "@timestamp": doc_ts,
                "cluster_name": cluster_name,
                "cluster_uuid": cluster_uuid,
                "source_node": {
                    "uuid": node_id,
                    "host": node.get('host'),
                    "transport_address": node.get('transport_address'),
                    "ip": node.get('ip'),
                    "name": node.get('name'),
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
            if r_json_node_prev and node_id in r_json_node_prev['nodes']:
                old_time = r_json_node_prev['nodes'][node_id]["indices"]["search"]["query_time_in_millis"]
                new_time = node_data["node_stats"]["indices"]["search"]["query_time_in_millis"]
                query_time_current = new_time - old_time
                node_data["node_stats"]["indices"]["search"]["query_time_current"] = query_time_current
                #notice that "query_current" does not deliver correct data since it counts the *currently*
                #running queries rather than the additional queries ran
                query_total = node_data["node_stats"]["indices"]["search"]["query_total"]
                query_total_old = r_json_node_prev['nodes'][node_id]["indices"]["search"]["query_total"]
                query_count_delta = query_total - query_total_old
                node_data["node_stats"]["indices"]["search"]["query_count_delta"] = query_count_delta
                if query_count_delta != 0:
                    query_avg_time = query_time_current / query_count_delta
                else:
                    query_avg_time = 0
                node_data["node_stats"]["indices"]["search"]["query_avg_time"] = query_avg_time
                #shaig 4.3 adding data for io_stats
                old_write = r_json_node_prev['nodes'][node_id]["fs"]["io_stats"]["total"]["write_operations"]
                new_write = node_data["node_stats"]["fs"]["io_stats"]["total"]["write_operations"]
                write_ops_current = new_write - old_write
                node_data["node_stats"]["fs"]["io_stats"]["total"]["write_ops_current"] = write_ops_current
                old_read = r_json_node_prev['nodes'][node_id]["fs"]["io_stats"]["total"]["read_operations"]
                new_read = node_data["node_stats"]["fs"]["io_stats"]["total"]["read_operations"]
                read_ops_current = new_read - old_read
                node_data["node_stats"]["fs"]["io_stats"]["total"]["read_ops_current"] = read_ops_current
            metric_docs.append(node_data)
    except (requests.exceptions.Timeout, socket.timeout) as e:
        logger.error("[%s] Timeout received on trying to get nodes stats" % (time.strftime("%Y-%m-%d %H:%M:%S")))

    # shaig 4.2 - adding data for current and average query time
    if r_json is not None:
        r_json_node_prev = r_json
    return metric_docs


def get_shard_data(routing_table):
    primaries = 0
    replicas = 0

    active_primaries = 0
    active_replicas = 0

    unassigned_primaries = 0
    unassigned_replicas = 0

    initializing = 0
    relocating = 0
    for shard in routing_table.items():
        key,unique_shard = shard
        for replica in unique_shard:
            isPrimary = bool(replica['primary'])
            state = replica['state']
            if isPrimary:
                primaries+= 1
                if state == 'STARTED':
                    active_primaries += 1
                elif  state == 'UNASSIGNED':
                    unassigned_primaries += 1
            else:
                replicas += 1
                if state == 'STARTED':
                    active_replicas += 1
                elif state == 'UNASSIGNED':
                    unassigned_replicas += 1
            if state == 'INITIALIZING':
                initializing += 1
            if state == 'RELOCATING':
                relocating += 1
    return {
    'total': primaries + replicas,
    'primaries': primaries,
    'replicas': replicas,
    'active_total' : active_primaries + active_replicas,
    'active_primaries': active_primaries,
    'active_replicas': active_replicas,

    'unassigned_total' : unassigned_primaries + unassigned_replicas,
    'unassigned_primaries': unassigned_primaries,
    'unassigned_replicas': unassigned_replicas,
    'initializing': initializing,
    'relocating': relocating
    }

# shaig 18.3 - adding data for index stats
def fetch_index_stats(routing_table,base_url='http://localhost:9200/',verify=True):
    metric_docs = []
#    global r_json_index_prev
    try:
        # getting timestamp
        utc_datetime = datetime.datetime.utcnow()
        ts = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z')
        # getting index stats for all indices
        response = requests.get(base_url + '_stats', timeout=(5, 5),verify=verify)
        if response.status_code != 200:
            logger.error("bad response while getting index stats. response is:\n"+response.json())
            return metric_docs
        index_stats = response.json()
        # creating index stats json
        indices = index_stats['indices']
        #AWS doesn't accept /_settings
        response = requests.get(base_url + '*/_settings', timeout=(5, 5), verify=verify)
        if response.status_code != 200:
            logger.error("bad response while getting index settings. response is:\n" + response.json())
            return metric_docs
        index_settings = response.json()
        if type(index_settings) is not dict:
            logger.error("bad index settings. response is:\n" + response.json())
            return metric_docs
        index_settings_ordered = dict(sorted(index_settings.items()))
        routing_table_ordered = dict(sorted(routing_table.items()))

        for index_name in indices:
            shards = get_shard_data(routing_table_ordered[index_name]['shards'])
            #logger.info("Building log for index " + index_name)
            index_data = {
                "timestamp": ts ,
                "cluster_uuid": cluster_uuid,
                "type": "index_stats",
                "created" : index_settings_ordered[index_name]['settings']['index']['creation_date']
                }
            index_data['index_stats'] = indices[index_name]
            index_data['index_stats']['index'] = index_name

            index_data['index_stats']['shards'] = shards


            #TODO - implement index prev
            # if r_json_index_prev:
            #     try:
            #         prev_data = r_json_index_prev['indices'][index_name]
            #         calc_index_delta_statistics(index_data, prev_data)
            #     except KeyError:
            #         index_data['index_stats']["missing_previous"] = True
            metric_docs.append(index_data)
            # creating indices stats json
        summary = index_stats["_all"]
        summary_data = {
                "timestamp": ts,
                "cluster_uuid": cluster_uuid,
                "type": "indices_stats"
                }
        summary_data["indices_stats"] = {}
        summary_data["indices_stats"]["_all"] = summary
        metric_docs.append(summary_data)
    except (requests.exceptions.Timeout, socket.timeout) as e:
        logger.error("[%s] Timeout received on getting index stats" % (time.strftime("%Y-%m-%d %H:%M:%S")))

    #r_json_index_prev = r_json
    return metric_docs
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


def poll_metrics(cluster_host, monitor, monitor_host,health,verify,auth_token,cloud_provider):
    cluster_stats, node_stats,index_stats = get_all_data(cluster_host,health,verify,cloud_provider)
    if monitor == 'elasticsearch':
        into_elasticsearch(monitor_host, cluster_stats, node_stats,index_stats,auth_token)
    elif monitor == 'signalfx':
        into_signalfx(monitor_host, cluster_stats, node_stats)


def get_all_data(cluster_host,health,verify,cloud_provider):
    cluster_stats,routing_table = fetch_cluster_stats(cluster_host,health,verify,cloud_provider)
    node_stats = fetch_nodes_stats(cluster_host,verify)
    if type(routing_table) is not dict:
        index_stats = []
    else:
        index_stats = fetch_index_stats(routing_table,cluster_host,verify)
    return cluster_stats, node_stats,index_stats


def into_signalfx(sfx_key, cluster_stats, node_stats):
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
                                             'node_name': source_node.get('name'),
                                             'node_host': source_node.get('host'),
                                             'node_host': source_node.get('ip'),
                                             'node_uuid': source_node.get('uuid'),
                                             'cluster_name': source_node.get('uuid'),
                                             }
                                         }])
    ingest.stop()

def into_elasticsearch(monitor_host, cluster_stats, node_stats,index_stats,auth_token):
    utc_datetime = datetime.datetime.utcnow()
    index_name = indexPrefix + str(utc_datetime.strftime('%Y.%m.%d'))

    cluster_stats_data = ['{"index":{"_index":"'+index_name+'","_type":"_doc"}}\n' + json.dumps(o) for o in cluster_stats]
    node_stats_data = ['{"index":{"_index":"'+index_name+'","_type":"_doc"}}\n' + json.dumps(with_type(o, 'node_stats')) for o in node_stats]
    data = node_stats_data + cluster_stats_data
    if index_stats is not None:
        index_stats_data = ['{"index":{"_index":"' + index_name + '","_type":"_doc"}}\n' + json.dumps(
            o)  for o in index_stats]
        data += index_stats_data
    if len(data) > 0:
        try:
            headers = {'Content-Type': 'application/x-ndjson'}
            if auth_token is not None:
                headers['X-Auth-Token'] = auth_token
            bulk_response = requests.post(monitor_host + '_bulk',
                                          data='\n'.join(data),
                                          headers=headers,
                                          timeout=(30, 30))
            assert_http_status(bulk_response)
            for item in bulk_response.json()["items"]:
                if item.get("index") and item.get("index").get("status") != 201:
                    click.echo(json.dumps(item.get("index").get("error")))
                    click.echo(cluster_stats_data)
                else:
                    click.echo(json.dumps(item.get("index").get("status")))
        except (requests.exceptions.Timeout, socket.timeout):
            logger.error("[%s] Timeout received while pushing collected metrics to Elasticsearch" % (time.strftime("%Y-%m-%d %H:%M:%S")))


def with_type(o, _type):
    o["type"] = _type
    return o


@click.command()
@click.argument('monitor-host', default=monitoringCluster)
@click.argument('monitor', default='elasticsearch')
@click.argument('cluster-host', default='http://localhost:9200/')
@click.option('--interval', default=10, help='Interval (in seconds) to run this')
@click.option('--index-prefix', default='', help='Index prefix for Elastic monitor')
@click.option('--generate-templates', default=False)
@click.option('--health-flag', default=True)
@click.option('--verify-flag', default=True)

def main(monitor_host, monitor, cluster_host,interval, index_prefix,generate_templates,health_flag,verify_flag ):
    global cluster_uuid, indexPrefix
    verify = verify_flag == 'True'
    if not verify:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    health = health_flag == 'True'
    monitored_cluster = os.environ.get('ES_METRICS_CLUSTER_URL', cluster_host)
    auth_token = os.environ.get('ES_METRICS_MONITORING_AUTH_TOKEN')
    cloud_provider = os.environ.get('ES_METRICS_CLOUD_PROVIDER')
    if cloud_provider is not None and cloud_provider not in ['Amazon Elasticsearch','Elastic Cloud']:
        click.echo('supported cloud providers are "Amazon Elasticsearch" and "Elastic Cloud"')
        sys.exit(1)
    if ',' in monitored_cluster:
        cluster_hosts_source = monitored_cluster.split(',')
    else:
        cluster_hosts_source = [monitored_cluster]
    cluster_hosts = []
    for cluster in cluster_hosts_source:
        if not cluster.endswith("/"):
            cluster_hosts.append(cluster + "/")
        else:
            cluster_hosts.append(cluster)

    indexPrefix = index_prefix or indexPrefix

    click.echo('[%s] Monitoring %s into %s at %s' % (time.strftime("%Y-%m-%d %H:%M:%S"), monitored_cluster, monitor, monitor_host))

    init = False
    while not init:
        try:
            response = requests.get(random.choice(cluster_hosts), timeout=(5, 5),verify=verify)
            assert_http_status(response)
            cluster_uuid = response.json().get('cluster_uuid')
            init = True
            if monitor == 'elasticsearch' and generate_templates:
                try:
                    create_templates()
                except (requests.exceptions.Timeout, socket.timeout, requests.exceptions.ConnectionError):
                    click.echo("[%s] Timeout received when trying to put template" % (time.strftime("%Y-%m-%d %H:%M:%S")))
            elif monitor == 'signalfx':
                import signalfx
        except (requests.exceptions.Timeout, socket.timeout, requests.exceptions.ConnectionError) as e:
            click.echo("[%s] Timeout received on trying to get cluster uuid" % (time.strftime("%Y-%m-%d %H:%M:%S")))
            sys.exit(1)

    click.echo('[%s] Started' % (time.strftime("%Y-%m-%d %H:%M:%S"),))

    recurring = interval > 0
    if not recurring:
        poll_metrics(cluster_host, monitor, monitor_host,health,verify,auth_token,cloud_provider)
    else:
        try:
            nextRun = 0
            while True:
                if time.time() >= nextRun:
                    nextRun = time.time() + interval
                    now = time.time()

                    poll_metrics(random.choice(cluster_hosts), monitor, monitor_host,health,verify,auth_token,cloud_provider)

                    elapsed = time.time() - now
                    # click.echo("[%s] Total Elapsed Time: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), elapsed))
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
                logger.error(e)
                os._exit(1)


if __name__ == '__main__':
    main()
