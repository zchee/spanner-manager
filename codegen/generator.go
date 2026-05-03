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
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"text/template"

	"golang.org/x/tools/imports"
	gofumpt "mvdan.cc/gofumpt/format"
)

const (
	headerTemplateName    = "header.go.tmpl"
	spannerDBTemplateName = "spanner_db.go.tmpl"
	typeTemplateName      = "type.go.tmpl"

	generatedSourceSnippetMaxBytes = 4096
	generatedSourceSnippetMaxLines = 20
)

//go:embed languages/go/templates/*.tmpl
var embeddedTemplateFS embed.FS

var defaultTemplateFS = mustSubFS(embeddedTemplateFS, "languages/go/templates")

var importsProcessMu sync.Mutex

// Options configures the code generator.
type Options struct {
	OutDir          string
	PackageName     string
	Language        string
	IgnoreTables    []string
	IncludeTables   []string
	Suffix          string
	TemplatePath    string
	Config          *Config
	SingularizeRows bool
	RowSuffix       string
}

// Generator orchestrates code generation from a schema.
type Generator struct {
	opts       Options
	templateFS fs.FS
	types      []Type
}

// NewGenerator creates a new Generator.
func NewGenerator(opts Options) *Generator {
	if opts.Suffix == "" {
		opts.Suffix = ".spanner.go"
	}
	if opts.Language == "" {
		opts.Language = "go"
	}
	if opts.PackageName == "" {
		opts.PackageName = filepath.Base(opts.OutDir)
	}

	templateFS := defaultTemplateFS
	if opts.TemplatePath != "" {
		templateFS = os.DirFS(opts.TemplatePath)
	}

	return &Generator{
		opts:       opts,
		templateFS: templateFS,
	}
}

// Generate generates code for all types in the schema.
func (g *Generator) Generate(schema *Schema) error {
	if err := os.MkdirAll(g.opts.OutDir, 0o755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	g.applyGeneratedNames(schema)

	// Apply custom type mappings from config.
	if g.opts.Config != nil {
		if err := g.applyConfig(schema); err != nil {
			return fmt.Errorf("applying config: %w", err)
		}
	}

	selectedTypes, err := g.filterTypes(schema.Types)
	if err != nil {
		return fmt.Errorf("selecting tables: %w", err)
	}
	filteredSchema := &Schema{Types: selectedTypes}
	g.types = filteredSchema.Types
	if err := g.removeLegacyHeaderFile(); err != nil {
		return fmt.Errorf("removing legacy header: %w", err)
	}

	// Generate spanner_db file.
	if err := g.generateSpannerDB(); err != nil {
		return fmt.Errorf("generating spanner_db: %w", err)
	}

	// Generate per-type files.
	for i := range filteredSchema.Types {
		if err := g.generateType(&filteredSchema.Types[i]); err != nil {
			return fmt.Errorf("generating type %s: %w", filteredSchema.Types[i].Name, err)
		}
	}

	return nil
}

func (g *Generator) applyGeneratedNames(schema *Schema) {
	inflections := []Inflection(nil)
	if g.opts.Config != nil {
		inflections = g.opts.Config.Inflections
	}

	for i := range schema.Types {
		rowName := generatedRowName(schema.Types[i].Table, g.opts.SingularizeRows, g.opts.RowSuffix, inflections)
		schema.Types[i].Name = rowName
		schema.Types[i].FileNameBase = generatedFileNameBase(rowName)
		refreshTypeMetadata(&schema.Types[i])
	}
}

func (g *Generator) applyConfig(schema *Schema) error {
	if g.opts.Config == nil {
		return nil
	}

	tableConfigs := make(map[string]*TableConfig)
	for i := range g.opts.Config.Tables {
		tc := &g.opts.Config.Tables[i]
		tableConfigs[tc.Name] = tc
	}

	seenTables := make(map[string]bool)
	for i := range schema.Types {
		tc, ok := tableConfigs[schema.Types[i].Table]
		if !ok {
			continue
		}
		seenTables[schema.Types[i].Table] = true

		if tc.RowName != "" {
			schema.Types[i].Name = tc.RowName
			schema.Types[i].FileNameBase = generatedFileNameBase(tc.RowName)
		}

		colConfigs := make(map[string]ColumnConfig)
		for _, cc := range tc.Columns {
			colConfigs[cc.Name] = cc
		}

		seenColumns := make(map[string]bool)
		for j := range schema.Types[i].Fields {
			cc, ok := colConfigs[schema.Types[i].Fields[j].ColumnName]
			if !ok {
				continue
			}
			seenColumns[cc.Name] = true
			if err := applyColumnConfig(&schema.Types[i].Fields[j], cc); err != nil {
				return fmt.Errorf("table %q column %q: %w", schema.Types[i].Table, cc.Name, err)
			}
		}

		for _, cc := range tc.Columns {
			if !seenColumns[cc.Name] {
				return fmt.Errorf("table %q config references unknown column %q", schema.Types[i].Table, cc.Name)
			}
		}
		refreshTypeMetadata(&schema.Types[i])
	}

	for _, tc := range g.opts.Config.Tables {
		if !seenTables[tc.Name] {
			return fmt.Errorf("config references unknown table %q", tc.Name)
		}
	}

	return nil
}

func applyColumnConfig(field *Field, cc ColumnConfig) error {
	customImports, err := importSpecsFromConfig(cc.Imports)
	if err != nil {
		return err
	}
	jsonImports, err := importSpecsFromConfig(cc.JSONTypeImports)
	if err != nil {
		return err
	}

	switch {
	case cc.JSONType != "":
		if field.IsArray {
			return fmt.Errorf("json_type is not supported for array columns")
		}

		var wrapper string
		switch field.BaseSpannerType {
		case "STRING":
			wrapper = "JSONString"
		case "JSON":
			wrapper = "JSONValue"
		default:
			return fmt.Errorf("json_type requires STRING or JSON column, got %s", field.SpannerType)
		}

		field.GoType = fmt.Sprintf("%s[%s]", wrapper, cc.JSONType)
		field.Imports = dedupeImportSpecs(append(
			append(inferImportSpecsFromTypeExpr(field.GoType), customImports...),
			jsonImports...,
		))
	case cc.CustomType != "":
		field.GoType = cc.CustomType
		field.Imports = dedupeImportSpecs(append(inferImportSpecsFromTypeExpr(field.GoType), customImports...))
	default:
		if len(customImports) > 0 || len(jsonImports) > 0 {
			return fmt.Errorf("imports require custom_type or json_type")
		}
	}

	return nil
}

func (g *Generator) filterTypes(types []Type) ([]Type, error) {
	include := make(map[string]bool)
	for _, table := range g.opts.IncludeTables {
		include[table] = true
	}

	ignore := make(map[string]bool)
	for _, table := range g.opts.IgnoreTables {
		ignore[table] = true
	}

	filtered := make([]Type, 0, len(types))
	seenIncluded := make(map[string]bool)
	for _, typ := range types {
		if len(include) > 0 && !include[typ.Table] {
			continue
		}
		seenIncluded[typ.Table] = true
		if ignore[typ.Table] {
			continue
		}
		filtered = append(filtered, typ)
	}

	for table := range include {
		if !seenIncluded[table] {
			return nil, fmt.Errorf("unknown table %q", table)
		}
	}

	if len(include) > 0 && len(filtered) == 0 {
		return nil, fmt.Errorf("no tables selected after applying filters")
	}

	return filtered, nil
}

func (g *Generator) generateSpannerDB() error {
	return g.writeTemplate("spanner_db"+g.opts.Suffix, spannerDBTemplateName, map[string]any{
		"PackageName": g.opts.PackageName,
		"Types":       g.types,
	})
}

func (g *Generator) generateType(t *Type) error {
	filename := t.FileNameBase
	if filename == "" {
		filename = strings.ToLower(t.Name)
	}
	return g.writeTemplate(filename+g.opts.Suffix, typeTemplateName, map[string]any{
		"PackageName": g.opts.PackageName,
		"Types":       g.types,
		"Type":        t,
	})
}

func (g *Generator) removeLegacyHeaderFile() error {
	path := filepath.Join(g.opts.OutDir, "spanner_header"+g.opts.Suffix)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (g *Generator) writeTemplate(filename, templateName string, data any) error {
	funcMap := template.FuncMap{
		"lowerFirst": lowerCamel,
		"commitTimestampExpr": func(field Field) string {
			expr, _ := commitTimestampExpr(field)
			return expr
		},
	}

	tmplText, err := fs.ReadFile(g.templateFS, templateName)
	if err != nil {
		return fmt.Errorf("reading template %s: %w", templateName, err)
	}
	headerText, err := fs.ReadFile(g.templateFS, headerTemplateName)
	if err != nil {
		return fmt.Errorf("reading template %s: %w", headerTemplateName, err)
	}

	header, err := executeTemplate(headerTemplateName, string(headerText), funcMap, data)
	if err != nil {
		return fmt.Errorf("executing template %s: %w", headerTemplateName, err)
	}
	body, err := executeTemplate(templateName, string(tmplText), funcMap, data)
	if err != nil {
		return fmt.Errorf("executing template %s: %w", templateName, err)
	}

	var buf bytes.Buffer
	buf.Write(mergeGeneratedFile(header, body))

	// Format Go code.
	if g.opts.Language == "go" {
		formatted, err := g.formatGoSource(filename, buf.Bytes())
		if err != nil {
			return fmt.Errorf("formatting %s: %w", filename, err)
		}
		buf.Reset()
		buf.Write(formatted)
	}

	path := filepath.Join(g.opts.OutDir, filename)
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fmt.Errorf("writing file %s: %w", path, err)
	}

	return nil
}

func executeTemplate(name, tmplText string, funcMap template.FuncMap, data any) ([]byte, error) {
	tmpl, err := template.New(name).Funcs(funcMap).Parse(tmplText)
	if err != nil {
		return nil, fmt.Errorf("parsing template %s: %w", name, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func mergeGeneratedFile(header, body []byte) []byte {
	headerParts := splitGeneratedSource(header)
	bodyParts := splitGeneratedSource(body)

	packageLine := headerParts.packageLine
	if len(packageLine) == 0 {
		packageLine = bodyParts.packageLine
	}

	sections := make([][]byte, 0, 4)
	if preamble := joinGeneratedSections(bodyParts.preamble, headerParts.preamble); len(preamble) != 0 {
		sections = append(sections, preamble)
	}
	if len(packageLine) != 0 {
		sections = append(sections, packageLine)
	}
	if imports := joinGeneratedSections(headerParts.imports, bodyParts.imports); len(imports) != 0 {
		sections = append(sections, imports)
	}
	if rest := joinGeneratedSections(headerParts.rest, bodyParts.rest); len(rest) != 0 {
		sections = append(sections, rest)
	}
	if len(sections) == 0 {
		return nil
	}
	return append(bytes.Join(sections, []byte("\n\n")), '\n')
}

type generatedSourceParts struct {
	preamble    []byte
	packageLine []byte
	imports     []byte
	rest        []byte
}

func splitGeneratedSource(src []byte) generatedSourceParts {
	var parts generatedSourceParts
	src = bytes.TrimLeft(src, "\n")
	if len(src) == 0 {
		return parts
	}

	offset := 0
	inBlockComment := false
	for offset < len(src) {
		lineStart := offset
		for offset < len(src) && src[offset] != '\n' {
			offset++
		}
		lineEnd := offset
		next := offset
		if next < len(src) && src[next] == '\n' {
			next++
		}

		line := bytes.TrimSpace(src[lineStart:lineEnd])
		if inBlockComment {
			if bytes.Contains(line, []byte("*/")) {
				inBlockComment = false
			}
			offset = next
			continue
		}
		switch {
		case len(line) == 0:
			offset = next
			continue
		case bytes.HasPrefix(line, []byte("//")):
			offset = next
			continue
		case bytes.HasPrefix(line, []byte("/*")):
			if !bytes.Contains(line, []byte("*/")) {
				inBlockComment = true
			}
			offset = next
			continue
		case bytes.HasPrefix(line, []byte("package ")):
			parts.preamble = bytes.Trim(src[:lineStart], "\n")
			parts.packageLine = bytes.TrimSpace(src[lineStart:lineEnd])
			parts.imports, parts.rest = extractImportSection(src[next:])
			parts.rest = bytes.TrimLeft(parts.rest, "\n")
			return parts
		default:
			parts.preamble = bytes.Trim(src[:lineStart], "\n")
			parts.imports, parts.rest = extractImportSection(src[lineStart:])
			parts.rest = bytes.TrimLeft(parts.rest, "\n")
			return parts
		}
	}

	parts.rest = bytes.Trim(src, "\n")
	return parts
}

func extractImportSection(src []byte) (imports, rest []byte) {
	offset := 0
	firstImportStart := -1
	lastImportEnd := 0

	for {
		candidateStart := offset
		significantStart, lineEnd, ok := nextSignificantLine(src, offset)
		if !ok {
			if firstImportStart < 0 {
				return nil, src
			}
			return bytes.Trim(src[firstImportStart:lastImportEnd], "\n"), src[lastImportEnd:]
		}
		trimmed := bytes.TrimSpace(src[significantStart:lineEnd])
		if !bytes.HasPrefix(trimmed, []byte("import ")) {
			if firstImportStart < 0 {
				return nil, src
			}
			return bytes.Trim(src[firstImportStart:lastImportEnd], "\n"), src[lastImportEnd:]
		}
		if firstImportStart < 0 {
			firstImportStart = candidateStart
		}
		declEnd := consumeImportDecl(src, significantStart)
		lastImportEnd = declEnd
		offset = declEnd
	}
}

func nextSignificantLine(src []byte, offset int) (lineStart, lineEnd int, ok bool) {
	inBlockComment := false
	for offset < len(src) {
		lineStart = offset
		for offset < len(src) && src[offset] != '\n' {
			offset++
		}
		lineEnd = offset
		if offset < len(src) && src[offset] == '\n' {
			offset++
		}

		line := bytes.TrimSpace(src[lineStart:lineEnd])
		if inBlockComment {
			if bytes.Contains(line, []byte("*/")) {
				inBlockComment = false
			}
			continue
		}
		switch {
		case len(line) == 0:
			continue
		case bytes.HasPrefix(line, []byte("//")):
			continue
		case bytes.HasPrefix(line, []byte("/*")):
			if !bytes.Contains(line, []byte("*/")) {
				inBlockComment = true
			}
			continue
		default:
			return lineStart, lineEnd, true
		}
	}
	return 0, 0, false
}

func consumeImportDecl(src []byte, start int) int {
	depth := 0
	seenParen := false
	inBlockComment := false
	offset := start
	for offset < len(src) {
		lineStart := offset
		for offset < len(src) && src[offset] != '\n' {
			offset++
		}
		lineEnd := offset
		if offset < len(src) && src[offset] == '\n' {
			offset++
		}
		line := src[lineStart:lineEnd]
		delta, hasParen, nextInBlockComment := importParenDelta(line, inBlockComment)
		depth += delta
		inBlockComment = nextInBlockComment
		if hasParen {
			seenParen = true
		}
		if !seenParen || depth <= 0 {
			return offset
		}
	}
	return len(src)
}

func importParenDelta(line []byte, inBlockComment bool) (delta int, hasParen, nextInBlockComment bool) {
	nextInBlockComment = inBlockComment
	for i := 0; i < len(line); i++ {
		if nextInBlockComment {
			if i+1 < len(line) && line[i] == '*' && line[i+1] == '/' {
				nextInBlockComment = false
				i++
			}
			continue
		}

		if i+1 < len(line) {
			switch {
			case line[i] == '/' && line[i+1] == '/':
				return delta, hasParen, nextInBlockComment
			case line[i] == '/' && line[i+1] == '*':
				nextInBlockComment = true
				i++
				continue
			}
		}

		switch line[i] {
		case '(', ')':
			hasParen = true
			if line[i] == '(' {
				delta++
			} else {
				delta--
			}
		case '\'', '"', '`':
			quote := line[i]
			for i++; i < len(line); i++ {
				if quote != '`' && line[i] == '\\' && i+1 < len(line) {
					i++
					continue
				}
				if line[i] == quote {
					break
				}
			}
		}
	}
	return delta, hasParen, nextInBlockComment
}

func joinGeneratedSections(parts ...[]byte) []byte {
	trimmed := make([][]byte, 0, len(parts))
	for _, part := range parts {
		part = bytes.Trim(part, "\n")
		if len(part) == 0 {
			continue
		}
		trimmed = append(trimmed, part)
	}
	if len(trimmed) == 0 {
		return nil
	}
	return bytes.Join(trimmed, []byte("\n\n"))
}

func (g *Generator) formatGoSource(filename string, src []byte) ([]byte, error) {
	modulePath, langVersion := detectGoModuleConfig(g.opts.OutDir)
	generatedFile := filepath.Join(g.opts.OutDir, filename)
	if absPath, err := filepath.Abs(generatedFile); err == nil {
		generatedFile = absPath
	}

	formatted, err := processImportsWithLocalPrefix(generatedFile, src, modulePath)
	if err != nil {
		return nil, fmt.Errorf("%w\nsource snippet:\n%s", err, generatedSourceSnippet(src))
	}

	formatted, err = gofumpt.Source(formatted, gofumpt.Options{
		LangVersion: langVersion,
		ModulePath:  modulePath,
		ExtraRules:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("gofumpt: %w\nsource snippet:\n%s", err, generatedSourceSnippet(formatted))
	}

	return formatted, nil
}

func generatedSourceSnippet(src []byte) string {
	truncated := false
	if len(src) > generatedSourceSnippetMaxBytes {
		src = src[:generatedSourceSnippetMaxBytes]
		truncated = true
	}

	lines := strings.Split(string(src), "\n")
	if len(lines) > generatedSourceSnippetMaxLines {
		lines = lines[:generatedSourceSnippetMaxLines]
		truncated = true
	}

	snippet := strings.Join(lines, "\n")
	if strings.TrimSpace(snippet) == "" {
		snippet = "<empty>"
	}
	if truncated {
		snippet += "\n..."
	}
	return snippet
}

func processImportsWithLocalPrefix(generatedFile string, src []byte, modulePath string) ([]byte, error) {
	importsProcessMu.Lock()
	defer importsProcessMu.Unlock()

	previousLocalPrefix := imports.LocalPrefix
	imports.LocalPrefix = modulePath
	defer func() {
		imports.LocalPrefix = previousLocalPrefix
	}()

	formatted, err := imports.Process(generatedFile, src, &imports.Options{
		TabWidth:  8,
		TabIndent: true,
		Comments:  true,
		Fragment:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("goimports: %w", err)
	}

	return formatted, nil
}

func detectGoModuleConfig(startDir string) (modulePath, langVersion string) {
	dir, err := filepath.Abs(startDir)
	if err != nil {
		dir = startDir
	}

	for dir != "" {
		data, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil {
			return parseGoModMetadata(data)
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", ""
}

func parseGoModMetadata(data []byte) (modulePath, langVersion string) {
	for line := range strings.SplitSeq(string(data), "\n") {
		fields := strings.Fields(strings.TrimSpace(line))
		if len(fields) < 2 {
			continue
		}

		switch fields[0] {
		case "module":
			if modulePath == "" {
				modulePath = strings.Trim(fields[1], "\"")
			}
		case "go":
			if langVersion == "" {
				langVersion = "go" + fields[1]
			}
		}

		if modulePath != "" && langVersion != "" {
			break
		}
	}

	return modulePath, langVersion
}

func mustSubFS(fsys fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(err)
	}
	return sub
}
