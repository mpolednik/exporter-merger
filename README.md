# exporter-merger

[![Build Status](https://travis-ci.org/rebuy-de/exporter-merger.svg?branch=master)](https://travis-ci.org/rebuy-de/exporter-merger)
[![license](https://img.shields.io/github/license/rebuy-de/exporter-merger.svg)]()
[![GitHub release](https://img.shields.io/github/release/rebuy-de/exporter-merger.svg)]()

Merges Prometheus metrics from multiple sources.

> **Development Status** *exporter-merger* is in an early development phase.
> Expect incompatible changes and abandoment at any time.

## But Why?!

> [prometheus/prometheus#3756](https://github.com/prometheus/prometheus/issues/3756)

## Usage

### Dynamic Endpoint Discovery

When configured, exporter-merger attempts to discover containers through Docker SDK. When running in container,
`/var/run/docker.sock` must be mounted to corresponding directory. The configuration is as follows (docker-compose):

```yaml
environment:
- MERGER_LABEL=traefik.frontend.rule
- MERGER_LABEL_CONFIG=/metrics:8080:Host:my-container /metrics:8080:Host:my-container2
```

This examples combines Traefik container discovery with automatic metric
merging. Dynamic endpoint discovery can only be enabled through command line args or environment variables.

It is important to note that dynamic discovery may lead to metrics artifacts - as containers die, counts
will lose continuity.

### Static Endpoint Discovery

*exporter-merger* needs a configuration file. Currently, nothing but URLs are accepted:

```yaml
exporters:
- url: http://localhost:9100/metrics
- url: http://localhost:9101/metrics
```

To start the exporter:

```
exporter-merger --config-path merger.yaml --listen-port 8080
```

> Static discovery disables dynamic discovery.

### Environment variables

Alternatively configuration can be passed via environment variables, here is relevant part of `exporter-merger -h` output:

```
      --listen-port int      Listen port for the HTTP server. (ENV:MERGER_PORT) (default 8080)
      --url stringSlice      URL to scrape. Can be speficied multiple times. (ENV:MERGER_URLS,space-seperated)
      --label labels            Label to match labels against
      --label-map stringSlice   Tuples of port:path:label to scrape. (ENV:MERGER_LABELS,space-seperated)
```

## Kubernetes

The exporter-merger is supposed to run as a sidecar. Here is an example config with [nginx-exporter](https://github.com/rebuy-de/nginx-exporter):

```yaml
apiVersion: apps/v1
kind: Deployment

metadata:
  name: my-nginx
  labels:
    app: my-nginx

spec:
  selector:
    matchLabels:
      app: my-nginx

  template:
    metadata:
      name: my-nginx
      labels:
        app: my-nginx
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"

    spec:
      containers:
      - name: "nginx"
        image: "my-nginx" # nginx image with modified config file

        volumeMounts:
        - name: mtail
          mountPath: /var/log/nginx/mtail

      - name: nginx-exporter
        image: quay.io/rebuy/nginx-exporter:v1.1.0
        ports:
        - containerPort: 9397
        env:
        - name: NGINX_ACCESS_LOGS
          value: /var/log/nginx/mtail/access.log
        - name: NGINX_STATUS_URI
          value: http://localhost:8888/nginx_status
        volumeMounts:
        - name: mtail
          mountPath: /var/log/nginx/mtail

      - name: exporter-merger
        image: quay.io/rebuy/exporter-merger:v0.2.0
        ports:
        - containerPort: 8080
        env:
        # space-separated list of URLs
        - name: MERGER_URLS
          value: http://localhost:9000/prometheus/metrics http://localhost:9397/metrics
        # default exposed port, change only if need other than default 8080
        # - name: MERGER_PORT
        #   value: 8080
```

## Planned Features

* Allow transforming of metrics from backend exporters.
  * eg add a prefix to the metric names
  * eg add labels to the metrics
* Allow dynamic adding of exporters.
