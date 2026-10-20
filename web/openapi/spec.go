// Package openapi exposes the versioned HTTP contract embedded in the dashboard.
package openapi

import _ "embed"

// V2YAML is the canonical OpenAPI 3.0.3 document.
//
//go:embed v2.yaml
var V2YAML []byte
