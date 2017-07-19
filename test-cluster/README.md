Running the test cluster is done via:

```bash
export ES_VERSION=5.5.0
docker build --build-arg ES_VERSION=$ES_VERSION -t code972/elasticsearch-test-cluster:v$ES_VERSION test-cluster
```

Then run the cluster using:

```bash
docker run -p 0.0.0.0:9200:9200 -t code972/elasticsearch-test-cluster:v$ES_VERSION
```
