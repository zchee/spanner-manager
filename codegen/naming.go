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

var commonInitialisms = map[string]struct{}{
	"ACL":   {},
	"API":   {},
	"ASCII": {},
	"CPU":   {},
	"CSS":   {},
	"DB":    {},
	"DDL":   {},
	"DML":   {},
	"DNS":   {},
	"EOF":   {},
	"GCP":   {},
	"GUID":  {},
	"HTML":  {},
	"HTTP":  {},
	"HTTPS": {},
	"ID":    {},
	"IP":    {},
	"JSON":  {},
	"LHS":   {},
	"QPS":   {},
	"RAM":   {},
	"RHS":   {},
	"RPC":   {},
	"SLA":   {},
	"SMTP":  {},
	"SQL":   {},
	"SSH":   {},
	"TCP":   {},
	"TLS":   {},
	"TTL":   {},
	"UDP":   {},
	"UI":    {},
	"UID":   {},
	"URI":   {},
	"URL":   {},
	"UTF8":  {},
	"UUID":  {},
	"VM":    {},
	"XML":   {},
	"XMPP":  {},
	"XSRF":  {},
	"XSS":   {},
}

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

func upperCamelIdentifier(s string) string {
	var b strings.Builder
	for part := range strings.SplitSeq(s, "_") {
		for _, word := range identifierWords(part) {
			b.WriteString(upperCamelWord(word))
		}
	}
	return b.String()
}

func lowerCamel(s string) string {
	var words []string
	for part := range strings.SplitSeq(s, "_") {
		words = append(words, identifierWords(part)...)
	}
	if len(words) == 0 {
		return ""
	}

	var b strings.Builder
	b.WriteString(lowerCamelWord(words[0]))
	for _, word := range words[1:] {
		b.WriteString(upperCamelWord(word))
	}
	return b.String()
}

func identifierWords(s string) []string {
	if s == "" {
		return nil
	}

	words := make([]string, 0, 4)
	start := 0
	for i := 1; i < len(s); i++ {
		prev := s[i-1]
		curr := s[i]
		var next byte
		if i+1 < len(s) {
			next = s[i+1]
		}

		if isUpperASCII(curr) && (isLowerASCII(prev) || isDigitASCII(prev)) {
			words = append(words, s[start:i])
			start = i
			continue
		}
		if isUpperASCII(prev) && isUpperASCII(curr) && next != 0 && isLowerASCII(next) {
			if next == 's' && (i+2 == len(s) || isUpperASCII(s[i+2])) {
				continue
			}
			words = append(words, s[start:i])
			start = i
		}
	}

	words = append(words, s[start:])
	return words
}

func upperCamelWord(word string) string {
	if normalized, ok := normalizeInitialismWord(word); ok {
		return normalized
	}
	if word == "" {
		return word
	}
	return strings.ToUpper(word[:1]) + strings.ToLower(word[1:])
}

func lowerCamelWord(word string) string {
	if normalized, ok := normalizeInitialismWord(word); ok {
		return strings.ToLower(normalized)
	}
	if word == "" {
		return word
	}
	return strings.ToLower(word[:1]) + word[1:]
}

func normalizeInitialismWord(word string) (string, bool) {
	upper := strings.ToUpper(word)
	if _, ok := commonInitialisms[upper]; ok {
		return upper, true
	}
	if before, ok := strings.CutSuffix(upper, "S"); ok {
		base := before
		if _, ok := commonInitialisms[base]; ok {
			return base + "s", true
		}
	}
	return "", false
}

func isUpperASCII(b byte) bool {
	return 'A' <= b && b <= 'Z'
}

func isLowerASCII(b byte) bool {
	return 'a' <= b && b <= 'z'
}

func isDigitASCII(b byte) bool {
	return '0' <= b && b <= '9'
}
