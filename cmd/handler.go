package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	docker "github.com/docker/docker/client"
	prom "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	log "github.com/sirupsen/logrus"
)

type Handler struct {
	exporters    []string
	labelConfigs []labelConfiguration
	label        string
	httpTimeout  int
}

type labelConfiguration struct {
	label string
	port  int
	path  string
}

func parseLabelConfiguration(labels []string) []labelConfiguration {
	lcs := make([]labelConfiguration, 0)

	for _, labelConfig := range labels {
		parts := strings.SplitN(labelConfig, ":", 3)
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			panic("Invalid port! Port should be value in range 1-65535")
		}
		lcs = append(lcs, labelConfiguration{
			path:  parts[0],
			port:  port,
			label: parts[2],
		})
	}

	return lcs
}

func NewHandler(httpTimeout int, exporters []string, label string, labelConfigs []string) Handler {
	return Handler{
		httpTimeout:  httpTimeout,
		exporters:    exporters,
		labelConfigs: parseLabelConfiguration(labelConfigs),
		label:        label,
	}
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.WithFields(log.Fields{
		"RequestURI": r.RequestURI,
		"UserAgent":  r.UserAgent(),
	}).Debug("handling new request")
	h.Merge(w)
}

func (h Handler) Merge(w io.Writer) {
	mfs := map[string]*prom.MetricFamily{}

	responses := make([]map[string]*prom.MetricFamily, 1024)
	responsesMu := sync.Mutex{}
	httpClientTimeout := time.Second * time.Duration(h.httpTimeout)

	wg := sync.WaitGroup{}
	for _, url := range h.exporterUrls() {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			log.WithField("url", u).Debug("getting remote metrics")
			httpClient := http.Client{Timeout: httpClientTimeout}
			resp, err := httpClient.Get(u)
			if err != nil {
				log.WithField("url", u).Errorf("HTTP connection failed: %v", err)
				return
			}
			defer resp.Body.Close()

			tp := new(expfmt.TextParser)
			part, err := tp.TextToMetricFamilies(resp.Body)
			if err != nil {
				log.WithField("url", u).Errorf("Parse response body to metrics: %v", err)
				return
			}
			responsesMu.Lock()
			responses = append(responses, part)
			responsesMu.Unlock()
		}(url)
	}
	wg.Wait()

	for _, part := range responses {
		for n, mf := range part {
			mfo, ok := mfs[n]
			if ok {
				mfo.Metric = append(mfo.Metric, mf.Metric...)
			} else {
				mfs[n] = mf
			}

		}
	}

	// Logic to merge metrics based on labels and types.
	for _, mf := range mfs {
		// Go through metrics of a metric family and merge the metrics in separate map.
		mergedMetricsTmp := map[string]*prom.Metric{}
		for _, metric := range mf.GetMetric() {
			var labels strings.Builder

			// Construct label identifier - merge together label strings forming a unique "bucket".
			for _, label := range metric.GetLabel() {
				labels.WriteString(label.String())
			}
			key := labels.String()

			_, ok := mergedMetricsTmp[key]
			if !ok {
				// Simple case: metric doesn't exist yet, create it.
				mergedMetricsTmp[key] = metric
			} else {
				// Hard case: if the key already exists, update it.
				mergeMetricValues(mergedMetricsTmp[key], *metric)
			}
		}

		// Transform the separate map to a list compatible with original format.
		mergedMetrics := []*prom.Metric{}
		for _, metric := range mergedMetricsTmp {
			mergedMetrics = append(mergedMetrics, metric)
		}

		// Replace the original metrics with merged metrics.
		mf.Metric = mergedMetrics
	}

	names := []string{}
	for n := range mfs {
		names = append(names, n)
	}
	sort.Strings(names)

	enc := expfmt.NewEncoder(w, expfmt.FmtText)
	for _, n := range names {
		err := enc.Encode(mfs[n])
		if err != nil {
			log.Error(err)
			return
		}
	}
}

func (h Handler) exporterUrls() []string {
	if len(h.exporters) > 0 {
		log.Infof("Exporters specified as URLs, using static configuration")
		return h.exporters
	}

	log.Infof("Using dynamic URL discovery")
	return h.urlsFromLabelConfigs()
}

func (h Handler) urlsFromLabelConfigs() []string {
	cli, err := docker.NewClientWithOpts(docker.WithVersion("1.38"))
	if err != nil {
		panic(err)
	}

	log.Debug("listing all containers")
	containers, err := cli.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		panic(err)
	}
	log.WithField("n", len(containers)).Infof("discovered n containers")

	urls := make([]string, 0)
	var labelMatch *labelConfiguration

	// Iterate over *all* running containers.
	for _, container := range containers {

		// First filter: expected label must be present.
		labelMatch = nil
		labelValue, ok := container.Labels[h.label]
		if !ok {
			continue
		}

		// Second filter: at least one labelConfiguration must match label value
		for _, lc := range h.labelConfigs {
			if strings.Compare(lc.label, labelValue) == 0 {
				labelMatch = &lc
				log.WithField("container", container.Names[0]).WithField("label", h.label).WithField("labelValue", labelValue).WithField("WantedLabelValue", lc.label).Debug("matched")
				break
			}
			log.WithField("container", container.Names[0]).WithField("label", h.label).WithField("labelValue", labelValue).WithField("WantedLabelValue", lc.label).Debug("no match")
		}

		// Skip container if we haven't found matching label pair.
		if labelMatch == nil {
			continue
		}

		// If the container has the label, we need to fetch its' name and construct URL in the following format:
		// http://${containerName}:${port}${path}
		scrapedURL := fmt.Sprintf("http://%s:%d%s", container.Names[0][1:], labelMatch.port, labelMatch.path)
		urls = append(urls, scrapedURL)
	}

	log.WithField("n", len(urls)).Infof("got n urls")
	return urls
}

func mergeMetricValues(dst *prom.Metric, src prom.Metric) {
	if dst.GetGauge() != nil && src.GetGauge() != nil {
		*(dst.GetGauge().Value) += src.GetGauge().GetValue()
	}
	if dst.GetCounter() != nil && src.GetCounter() != nil {
		*(dst.GetCounter().Value) += src.GetCounter().GetValue()
	}
	if dst.GetHistogram() != nil && src.GetHistogram() != nil {
		dstBuckets := dst.GetHistogram().GetBucket()
		srcBuckets := src.GetHistogram().GetBucket()
		for i := range dstBuckets {
			*(dstBuckets[i].CumulativeCount) += srcBuckets[i].GetCumulativeCount()
		}
		*(dst.GetHistogram().SampleCount) += src.GetHistogram().GetSampleCount()
		*(dst.GetHistogram().SampleSum) += src.GetHistogram().GetSampleSum()
	}
	// Summary merging is probably not done properly yet: we're not merging quantiles!
	if dst.GetSummary() != nil && src.GetSummary() != nil {
		*(dst.GetSummary().SampleCount) += src.GetSummary().GetSampleCount()
		*(dst.GetSummary().SampleSum) += src.GetSummary().GetSampleSum()
	}
	if dst.GetUntyped() != nil && src.GetUntyped() != nil {
		*(dst.GetUntyped().Value) += src.GetUntyped().GetValue()
	}
}
