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

import "strings"

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
	lower := strings.ToLower(name)
	for _, inflection := range inflections {
		if strings.EqualFold(inflection.Plural, lower) || strings.EqualFold(inflection.Plural, name) {
			return matchIdentifierCase(name, inflection.Singular)
		}
	}

	switch {
	case strings.HasSuffix(lower, "ies") && len(name) > 3:
		return name[:len(name)-3] + "y"
	case strings.HasSuffix(lower, "sses"),
		strings.HasSuffix(lower, "shes"),
		strings.HasSuffix(lower, "ches"),
		strings.HasSuffix(lower, "xes"),
		strings.HasSuffix(lower, "zes"):
		return name[:len(name)-2]
	case strings.HasSuffix(lower, "s") && !strings.HasSuffix(lower, "ss"):
		return name[:len(name)-1]
	default:
		return name
	}
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
