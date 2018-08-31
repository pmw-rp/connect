// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package input

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	yaml "gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

// PluginConstructor is a func that constructs a Benthos input plugin. These are
// plugins that are specific to certain use cases, experimental, private or
// otherwise unfit for widespread general use. Any number of plugins can be
// specified when using Benthos as a framework.
//
// The configuration object will be the result of the PluginConfigConstructor
// after overlaying the user configuration.
type PluginConstructor func(
	config interface{},
	manager types.Manager,
	logger log.Modular,
	metrics metrics.Type,
) (types.Input, error)

// PluginConfigConstructor is a func that returns a pointer to a new and fully
// populated configuration struct for a plugin type. It is valid to return a
// pointer to an empty struct (&struct{}{}) if no configuration fields are
// needed.
type PluginConfigConstructor func() interface{}

// PluginConfigSanitiser is a function that takes a configuration object for a
// plugin and returns a sanitised (minimal) version of it for printing in
// examples and plugin documentation.
//
// This function is useful for when a plugins configuration struct is very large
// and complex, but can sometimes be expressed in a more concise way without
// losing the original intent.
type PluginConfigSanitiser func(conf interface{}) interface{}

type pluginSpec struct {
	constructor     PluginConstructor
	confConstructor PluginConfigConstructor
	confSanitiser   PluginConfigSanitiser
	description     string
}

// pluginSpecs is a map of all input plugin type specs.
var pluginSpecs = map[string]pluginSpec{}

// RegisterPlugin registers a plugin by a unique name so that it can be
// constucted similar to regular inputs. A constructor for both the plugin
// itself as well as its configuration struct must be provided.
//
// WARNING: This API is experimental and could (is likely) to change.
func RegisterPlugin(
	typeString string,
	configConstructor PluginConfigConstructor,
	constructor PluginConstructor,
) {
	spec := pluginSpecs[typeString]
	spec.constructor = constructor
	spec.confConstructor = configConstructor
	pluginSpecs[typeString] = spec
}

// DocumentPlugin adds a description and an optional configuration sanitiser
// function to the definition of a registered plugin. This improves the
// documentation generated by PluginDescriptions.
func DocumentPlugin(
	typeString, description string,
	configSanitiser PluginConfigSanitiser,
) {
	spec := pluginSpecs[typeString]
	spec.description = description
	spec.confSanitiser = configSanitiser
	pluginSpecs[typeString] = spec
}

//------------------------------------------------------------------------------

var pluginHeader = `This document has been generated, do not edit it directly.

This document lists any input plugins that this flavour of Benthos offers beyond
the standard set.`

// PluginDescriptions generates and returns a markdown formatted document
// listing each registered plugin and an example configuration for it.
func PluginDescriptions() string {
	// Order alphabetically
	names := []string{}
	for name := range pluginSpecs {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Input Plugins\n")
	buf.WriteString(strings.Repeat("=", 13))
	buf.WriteString("\n\n")
	buf.WriteString(pluginHeader)
	buf.WriteString("\n\n")

	buf.WriteString("### Contents\n\n")
	for i, name := range names {
		buf.WriteString(fmt.Sprintf("%v. [`%v`](#%v)\n", i+1, name, name))
	}
	buf.WriteString("\n")

	// Append each description
	for i, name := range names {
		var confBytes []byte

		conf := NewConfig()
		conf.Type = name
		conf.Plugin = pluginSpecs[name].confConstructor()
		if confSanit, err := SanitiseConfig(conf); err == nil {
			confBytes, _ = yaml.Marshal(confSanit)
		}

		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		if confBytes != nil {
			buf.WriteString("\n``` yaml\n")
			buf.Write(confBytes)
			buf.WriteString("```\n")
		}
		if desc := pluginSpecs[name].description; len(desc) > 0 {
			buf.WriteString("\n")
			buf.WriteString(desc)
			buf.WriteString("\n")
		}
		if i != (len(names) - 1) {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

//------------------------------------------------------------------------------
