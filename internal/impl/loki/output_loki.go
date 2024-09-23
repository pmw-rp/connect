// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loki

import (
	"context"
	"fmt"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/loki/pkg/push"
	"github.com/grafana/loki/v3/clients/pkg/promtail/api"
	"github.com/grafana/loki/v3/clients/pkg/promtail/client"
	lokiflag "github.com/grafana/loki/v3/pkg/util/flagext"
	"github.com/prometheus/client_golang/prometheus"
	promconfig "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/redpanda-data/benthos/v4/public/service"
	"net/url"
	"regexp"
	"strings"
	"time"
)

func lokiOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("3.61.0").
		Summary("A (sub)stdout.").
		Description(`
Writes messages to stdout with headers.
`).Fields(LokiOutputConfigFields()...)
}

func init() {
	err := service.RegisterBatchOutput("loki", lokiOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput, batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}

			output, err = NewLokiOutputWriterFromConfig(conf, mgr.Logger())
			return
		})
	if err != nil {
		panic(err)
	}
}

// LokiOutputConfigFields returns the full suite of config fields for a
// kafka output using the franz-go client library.
func LokiOutputConfigFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewOutputMaxInFlightField(),
		service.NewObjectField("timestamps",
			service.NewBoolField("enabled").Default(true).Description("Whether to use timestamps from the record or just use time.Now()"),
			service.NewStringField("selector").Default("timestamp").Description("The name of the metadata to interpret as the record timestamp"),
			service.NewStringField("format").Default("2006-01-02 15:04:05,000")),
		service.NewStringListField("labels"),
		service.NewObjectField("conf",
			service.NewStringField("name").Optional(),
			service.NewStringField("url"),
			service.NewDurationField("batch_wait").Optional().Default("1s"),
			service.NewIntField("batch_size").Optional().Default(client.BatchSize),
			service.NewObjectField("client",
				service.NewObjectField("basic_auth",
					service.NewStringField("username").Optional(),
					service.NewStringField("username_file").Optional(),
					service.NewStringField("username_ref").Optional(),
					service.NewStringField("password").Optional().Secret(),
					service.NewStringField("password_file").Optional(),
					service.NewStringField("password_ref").Optional(),
				).Optional(),
				service.NewObjectField("authorization",
					service.NewStringField("type").Optional(),
					service.NewStringField("credentials").Optional().Secret(),
					service.NewStringField("credentials_file").Optional(),
					service.NewStringField("credentials_ref").Optional(),
				).Optional(),
				service.NewObjectField("oauth2",
					service.NewStringField("client_id").Optional(),
					service.NewStringField("client_secret").Optional().Secret(),
					service.NewStringField("client_secret_file").Optional(),
					service.NewStringField("client_secret_ref").Optional(),
					service.NewStringListField("scopes").Optional(),
					service.NewStringField("token_url").Optional(),
					service.NewStringMapField("endpoint_params").Optional(),
					service.NewObjectField("tls_config").Optional(),
					//service.NewObjectField("proxy_config",
					//	service.NewStringField("proxy_url").Optional(),
					//	service.NewStringField("no_proxy").Optional(),
					//	service.NewBoolField("proxy_from_environment").Optional(),
					//	service.NewStringMapField("proxy_connect_header").Optional(),
					//).Optional(),
				).Optional(),
				service.NewStringField("bearer_token").Optional().Secret(),
				service.NewStringField("bearer_token_file").Optional(),
				service.NewObjectField("tls_config").Optional(),
				service.NewBoolField("follow_redirects").Optional(),
				service.NewBoolField("enable_http2").Optional(),
				//service.NewObjectField("proxy_config",
				//	service.NewStringField("proxy_url").Optional(),
				//	service.NewStringField("no_proxy").Optional(),
				//	service.NewBoolField("proxy_from_environment").Optional(),
				//	service.NewStringMapField("proxy_connect_header").Optional(),
				//).Optional(),

			).Optional(),
			service.NewStringMapField("headers").Optional(),
			service.NewObjectField("backoff",
				service.NewDurationField("min_period").Optional().Default("500ms").Advanced(),
				service.NewDurationField("max_period").Optional().Default("5m").Advanced(),
				service.NewIntField("max_retries").Optional().Default(10).Advanced(),
			).Advanced(),
			service.NewStringMapField("external_labels").Optional(),
			service.NewDurationField("timeout").Optional().Default("10s"),
			service.NewStringField("tenant_id").Optional().Default(""),
			service.NewBoolField("drop_rate_limited_batches").Optional().Default(false),
		),
	}
}

//------------------------------------------------------------------------------

type TimestampConf struct {
	enabled  bool   `yaml:"enabled"`
	selector string `yaml:"selector"`
	format   string `yaml:"format"`
}

// LokiOutputWriter implements a Loki writer using the Promtail library.
type LokiOutputWriter struct {
	labels      []string
	labelsRegex *regexp.Regexp

	timestampConf TimestampConf

	conf                client.Config
	maxStreams          int
	maxLineSize         int
	maxLineSizeTruncate bool

	cl  client.Client
	log *service.Logger
}

func buildTLSConfig(conf *service.ParsedConfig) promconfig.TLSConfig {
	t := promconfig.TLSConfig{
		CA:                 "",
		Cert:               "",
		Key:                "",
		CAFile:             "",
		CertFile:           "",
		KeyFile:            "",
		CARef:              "",
		CertRef:            "",
		KeyRef:             "",
		ServerName:         "",
		InsecureSkipVerify: false,
		MinVersion:         0,
		MaxVersion:         0,
	}

	t.CA, _ = conf.FieldString("ca")
	t.Cert, _ = conf.FieldString("cert")
	key, err := conf.FieldString("key")
	if err == nil {
		t.Key = promconfig.Secret(key)
	}

	t.CAFile, _ = conf.FieldString("ca_file")
	t.CertFile, _ = conf.FieldString("cert_file")
	t.KeyFile, _ = conf.FieldString("key_file")

	t.CARef, _ = conf.FieldString("ca_ref")
	t.CertRef, _ = conf.FieldString("cert_ref")
	t.KeyRef, _ = conf.FieldString("key_ref")

	t.ServerName, _ = conf.FieldString("server_name")

	t.InsecureSkipVerify, _ = conf.FieldBool("insecure_skip_verify")

	minVersionString, err := conf.FieldString("min_version")
	if err == nil {
		minVersion, found := promconfig.TLSVersions[minVersionString]
		if found {
			t.MinVersion = minVersion
		}
	}

	maxVersionString, err := conf.FieldString("max_version")
	if err == nil {
		maxVersion, found := promconfig.TLSVersions[maxVersionString]
		if found {
			t.MaxVersion = maxVersion
		}
	}

	return t
}

func NewLokiConfigFromParsedConfig(conf *service.ParsedConfig, log *service.Logger) (*client.Config, error) {

	config := client.Config{
		Name:                   "",
		URL:                    flagext.URLValue{},
		BatchWait:              1 * time.Second,
		BatchSize:              1024 * 1024,
		Client:                 promconfig.DefaultHTTPClientConfig,
		Headers:                make(map[string]string),
		BackoffConfig:          backoff.Config{MinBackoff: 500 * time.Millisecond, MaxBackoff: 5 * time.Minute, MaxRetries: 10},
		ExternalLabels:         lokiflag.LabelSet{},
		Timeout:                10 * time.Second,
		TenantID:               "",
		DropRateLimitedBatches: false,
	}

	var err error

	config.Name, err = conf.FieldString("name")
	if err != nil {
		return nil, err
	}

	rawUrl, err := conf.FieldString("url")
	if err != nil {
		return nil, err
	}
	parsedUrl, err := url.Parse(rawUrl)
	if err != nil {
		return nil, err
	}
	config.URL = flagext.URLValue{URL: parsedUrl}

	config.BatchWait, err = conf.FieldDuration("batch_wait")
	if err != nil {
		return nil, err
	}

	config.BatchSize, err = conf.FieldInt("batch_size")
	if err != nil {
		return nil, err
	}

	if conf.Contains("client", "basic_auth", "username") ||
		conf.Contains("client", "basic_auth", "username_file") ||
		conf.Contains("client", "basic_auth", "username_ref") {
		auth := promconfig.BasicAuth{
			Username:     "",
			UsernameFile: "",
			UsernameRef:  "",
			Password:     "",
			PasswordFile: "",
			PasswordRef:  "",
		}
		auth.Username, _ = conf.FieldString("client", "basic_auth", "username")
		auth.UsernameFile, _ = conf.FieldString("client", "basic_auth", "username_file")
		auth.UsernameRef, _ = conf.FieldString("client", "basic_auth", "username_ref")

		password, err := conf.FieldString("client", "basic_auth", "password")
		if err == nil {
			auth.Password = promconfig.Secret(password)
		}
		auth.PasswordFile, _ = conf.FieldString("client", "basic_auth", "password_file")
		auth.PasswordRef, _ = conf.FieldString("client", "basic_auth", "password_ref")

		config.Client.BasicAuth = &auth
	}

	if conf.Contains("client", "authorization", "type") {
		auth := promconfig.Authorization{
			Type:            "",
			Credentials:     "",
			CredentialsFile: "",
			CredentialsRef:  "",
		}
		auth.Type, _ = conf.FieldString("client", "authorization", "type")
		credentials, err := conf.FieldString("client", "authorization", "credentials")
		if err == nil {
			auth.Credentials = promconfig.Secret(credentials)
		}
		auth.CredentialsFile, _ = conf.FieldString("client", "authorization", "credentials_file")
		auth.CredentialsRef, _ = conf.FieldString("client", "authorization", "credentials_ref")

		config.Client.Authorization = &auth
	}

	if conf.Contains("client", "oauth2", "client_id") {
		auth := promconfig.OAuth2{
			ClientID:         "",
			ClientSecret:     "",
			ClientSecretFile: "",
			ClientSecretRef:  "",
			Scopes:           nil,
			TokenURL:         "",
			EndpointParams:   nil,
			TLSConfig:        promconfig.TLSConfig{},
			ProxyConfig:      promconfig.ProxyConfig{},
		}
		auth.ClientID, _ = conf.FieldString("client", "oauth2", "client_id")
		clientSecret, err := conf.FieldString("client", "oauth2", "client_secret")
		if err == nil {
			auth.ClientSecret = promconfig.Secret(clientSecret)
		}
		auth.ClientSecretFile, _ = conf.FieldString("client", "oauth2", "client_secret_file")
		auth.ClientSecretRef, _ = conf.FieldString("client", "oauth2", "client_secret_ref")
		auth.Scopes, _ = conf.FieldStringList("client", "oauth2", "scopes")
		auth.TokenURL, _ = conf.FieldString("client", "oauth2", "token_url")
		auth.EndpointParams, _ = conf.FieldStringMap("client", "oauth2", "endpoint_params")
		auth.TLSConfig = buildTLSConfig(conf.Namespace("client", "oauth2", "tls_config"))
		//auth.ProxyConfig =

		config.Client.OAuth2 = &auth
	}

	bearerToken, err := conf.FieldString()
	if err == nil {
		config.Client.BearerToken = promconfig.Secret(bearerToken)
	}

	config.Client.BearerTokenFile, _ = conf.FieldString("bearer_token_file")

	config.Client.TLSConfig = buildTLSConfig(conf.Namespace("tls_config"))

	config.Headers, err = conf.FieldStringMap("headers")
	if err != nil {
		return nil, err
	}

	config.BackoffConfig.MinBackoff, err = conf.FieldDuration("backoff", "min_period")
	if err != nil {
		return nil, err
	}

	config.BackoffConfig.MaxBackoff, err = conf.FieldDuration("backoff", "max_period")
	if err != nil {
		return nil, err
	}

	config.BackoffConfig.MaxRetries, err = conf.FieldInt("backoff", "max_retries")
	if err != nil {
		return nil, err
	}

	externalLabels, err := conf.FieldStringMap("external_labels")
	if err != nil {
		return nil, err
	}
	config.ExternalLabels = lokiflag.LabelSet{}
	for k, v := range externalLabels {
		err := config.ExternalLabels.Set(fmt.Sprintf("%v=%v", k, v))
		if err != nil {
			return nil, err
		}
	}

	config.TenantID, err = conf.FieldString("tenant_id")
	if err != nil {
		return nil, err
	}

	config.DropRateLimitedBatches, err = conf.FieldBool("drop_rate_limited_batches")
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// NewLokiOutputWriterFromConfig attempts to instantiate a LokiOutputWriter from
// a parsed config.
func NewLokiOutputWriterFromConfig(conf *service.ParsedConfig, log *service.Logger) (*LokiOutputWriter, error) {
	var f LokiOutputWriter

	config, err := NewLokiConfigFromParsedConfig(conf.Namespace("conf"), log)
	if err != nil {
		return nil, err
	}
	f.conf = *config

	// Timestamps
	if enabled, err := conf.FieldBool("timestamps", "enabled"); err == nil {
		f.timestampConf.enabled = enabled
		if f.timestampConf.enabled {
			if selector, err := conf.FieldString("timestamps", "selector"); err == nil {
				f.timestampConf.selector = selector
			} else {
				return nil, err
			}
			if format, err := conf.FieldString("timestamps", "format"); err == nil {
				f.timestampConf.format = format
			} else {
				return nil, err
			}
		}
	} else {
		return nil, err
	}

	// Labels
	f.labels, _ = conf.FieldStringList("labels")
	var regex strings.Builder
	for i, label := range f.labels {
		if i > 0 {
			regex.WriteString("|")
		}
		regex.WriteString("(^")
		regex.WriteString(label)
		regex.WriteString("$)")
	}
	compiledRegex, err := regexp.Compile(regex.String())
	if err != nil {
		return nil, err
	}

	f.labelsRegex = compiledRegex
	f.log = log

	return &f, nil
}

type LoggerAdaptor struct {
	log *service.Logger
}

func (l *LoggerAdaptor) Log(keyvals ...interface{}) error {
	l.log.Errorf("%v", keyvals)
	return nil
}

// Connect to the target seed brokers.
func (f *LokiOutputWriter) Connect(ctx context.Context) error {
	registerer := prometheus.NewRegistry()
	metrics := client.NewMetrics(registerer)
	_ = metrics
	la := LoggerAdaptor{log: f.log}
	cl, err := client.New(metrics, f.conf, f.maxStreams, f.maxLineSize, f.maxLineSizeTruncate, &la)
	if err != nil {
		return err
	}
	f.cl = cl
	return nil
}

func (f *LokiOutputWriter) buildEntry(message *service.Message) (*api.Entry, error) {

	content, err := message.AsBytes()
	if err != nil {
		return nil, err
	}
	line := string(content)

	labels := make(model.LabelSet)
	metadata := make(push.LabelsAdapter, 0)
	parsed := make(push.LabelsAdapter, 0)

	var timestamp time.Time

	err = message.MetaWalk(func(k string, v string) error {
		if f.labelsRegex.Match([]byte(k)) {
			labels[model.LabelName(k)] = model.LabelValue(v)
		} else if f.timestampConf.enabled && k == f.timestampConf.selector {
			timestamp, err = time.Parse(f.timestampConf.format, v)
			if err != nil {
				return err
			}
		} else {
			metadata = append(metadata, push.LabelAdapter{
				Name:  k,
				Value: v,
			})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	if f.timestampConf.enabled == false {
		timestamp = time.Now()
	}

	return &api.Entry{
		Labels: labels,
		Entry: push.Entry{
			Timestamp:          timestamp,
			Line:               line,
			StructuredMetadata: metadata,
			Parsed:             parsed,
		}}, nil
}

// WriteBatch attempts to write a batch of messages to the target topics.
func (f *LokiOutputWriter) WriteBatch(ctx context.Context, messages service.MessageBatch) error {
	for _, message := range messages {
		entry, err := f.buildEntry(message)
		if err != nil {
			return err
		}
		f.cl.Chan() <- *entry
	}
	return nil
}

// Close underlying connections.
func (f *LokiOutputWriter) Close(ctx context.Context) error {
	f.cl.Stop()
	return nil
}
