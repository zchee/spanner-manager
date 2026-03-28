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
	"go/format"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"text/template"
)

const (
	headerTemplateName    = "header.go.tmpl"
	spannerDBTemplateName = "spanner_db.go.tmpl"
	typeTemplateName      = "type.go.tmpl"
)

//go:embed languages/go/templates/*.tmpl
var embeddedTemplateFS embed.FS

var defaultTemplateFS = mustSubFS(embeddedTemplateFS, "languages/go/templates")

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

	// Generate header file.
	if err := g.generateHeader(filteredSchema); err != nil {
		return fmt.Errorf("generating header: %w", err)
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

func (g *Generator) generateHeader(schema *Schema) error {
	return g.writeTemplate("spanner_header"+g.opts.Suffix, headerTemplateName, map[string]any{
		"PackageName": g.opts.PackageName,
		"Types":       schema.Types,
	})
}

func (g *Generator) generateSpannerDB() error {
	return g.writeTemplate("spanner_db"+g.opts.Suffix, spannerDBTemplateName, map[string]any{
		"PackageName": g.opts.PackageName,
	})
}

func (g *Generator) generateType(t *Type) error {
	filename := t.FileNameBase
	if filename == "" {
		filename = strings.ToLower(t.Name)
	}
	return g.writeTemplate(filename+g.opts.Suffix, typeTemplateName, map[string]any{
		"PackageName": g.opts.PackageName,
		"Type":        t,
	})
}

func (g *Generator) writeTemplate(filename, templateName string, data any) error {
	funcMap := template.FuncMap{
		"lowerFirst": func(s string) string {
			if s == "" {
				return s
			}
			return strings.ToLower(s[:1]) + s[1:]
		},
		"commitTimestampExpr": func(field Field) string {
			expr, _ := commitTimestampExpr(field)
			return expr
		},
	}

	tmplText, err := fs.ReadFile(g.templateFS, templateName)
	if err != nil {
		return fmt.Errorf("reading template %s: %w", templateName, err)
	}

	tmpl, err := template.New(templateName).Funcs(funcMap).Parse(string(tmplText))
	if err != nil {
		return fmt.Errorf("parsing template %s: %w", templateName, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("executing template: %w", err)
	}

	// Format Go code.
	if g.opts.Language == "go" {
		formatted, err := format.Source(buf.Bytes())
		if err != nil {
			// Write unformatted on format error for debugging.
			formatted = buf.Bytes()
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

func mustSubFS(fsys fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(err)
	}
	return sub
}
