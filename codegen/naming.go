// Copyright 2026 The spanner-manager Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package codegen

import (
	"strings"

	pluralize "github.com/gertd/go-pluralize"
)

var defaultPluralizer = pluralize.NewClient()

func generatedRowName(tableName string, singularizeRows bool, rowSuffix string, inflections []Inflection) string {
	name := snakeToCamel(tableName)
	if singularizeRows {
		name = singularizeIdentifier(name, inflections)
	}
	if rowSuffix != "" && !strings.HasSuffix(name, rowSuffix) {
		name += rowSuffix
	}
	return name
}

func generatedFileNameBase(rowName string) string {
	return strings.ToLower(rowName)
}

func singularizeIdentifier(name string, inflections []Inflection) string {
	if name == "" {
		return name
	}

	if singular, ok := applyCustomInflections(name, inflections); ok {
		return singular
	}

	return defaultPluralizer.Singular(name)
}

func applyCustomInflections(name string, inflections []Inflection) (string, bool) {
	lower := strings.ToLower(name)
	for _, inflection := range inflections {
		if inflection.Singular == "" || inflection.Plural == "" {
			continue
		}

		pluralLower := strings.ToLower(inflection.Plural)
		if !strings.HasSuffix(lower, pluralLower) {
			continue
		}

		offset := len(name) - len(inflection.Plural)
		return name[:offset] + matchIdentifierCase(name[offset:], inflection.Singular), true
	}

	return "", false
}

func matchIdentifierCase(existing, replacement string) string {
	if replacement == "" {
		return replacement
	}
	if strings.ToUpper(existing) == existing {
		return strings.ToUpper(replacement)
	}
	if strings.ToLower(existing) == existing {
		return strings.ToLower(replacement)
	}
	return strings.ToUpper(replacement[:1]) + replacement[1:]
}
