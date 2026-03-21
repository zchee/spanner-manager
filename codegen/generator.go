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

//go:embed templates/*.tmpl
var embeddedTemplateFS embed.FS

var defaultTemplateFS = mustSubFS(embeddedTemplateFS, "templates")

// Options configures the code generator.
type Options struct {
	OutDir       string
	PackageName  string
	Language     string
	IgnoreTables []string
	Suffix       string
	TemplatePath string
	Config       *Config
}

// Generator orchestrates code generation from a schema.
type Generator struct {
	opts       Options
	templateFS fs.FS
}

// NewGenerator creates a new Generator.
func NewGenerator(opts Options) *Generator {
	if opts.Suffix == "" {
		opts.Suffix = ".yo.go"
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

	ignoreTables := make(map[string]bool)
	for _, t := range g.opts.IgnoreTables {
		ignoreTables[t] = true
	}

	// Apply custom type mappings from config.
	if g.opts.Config != nil {
		g.applyConfig(schema)
	}

	// Generate header file.
	if err := g.generateHeader(schema); err != nil {
		return fmt.Errorf("generating header: %w", err)
	}

	// Generate spanner_db file.
	if err := g.generateSpannerDB(); err != nil {
		return fmt.Errorf("generating spanner_db: %w", err)
	}

	// Generate per-type files.
	for _, t := range schema.Types {
		if ignoreTables[t.Table] {
			continue
		}
		if err := g.generateType(&t); err != nil {
			return fmt.Errorf("generating type %s: %w", t.Name, err)
		}
	}

	return nil
}

func (g *Generator) applyConfig(schema *Schema) {
	if g.opts.Config == nil {
		return
	}

	tableConfigs := make(map[string]*TableConfig)
	for i := range g.opts.Config.Tables {
		tc := &g.opts.Config.Tables[i]
		tableConfigs[tc.Name] = tc
	}

	for i := range schema.Types {
		tc, ok := tableConfigs[schema.Types[i].Table]
		if !ok {
			continue
		}

		colConfigs := make(map[string]string)
		for _, cc := range tc.Columns {
			colConfigs[cc.Name] = cc.CustomType
		}

		for j := range schema.Types[i].Fields {
			if customType, ok := colConfigs[schema.Types[i].Fields[j].ColumnName]; ok {
				schema.Types[i].Fields[j].GoType = customType
			}
		}
	}
}

func (g *Generator) generateHeader(schema *Schema) error {
	return g.writeTemplate("yo_header"+g.opts.Suffix, headerTemplateName, map[string]any{
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
	return g.writeTemplate(strings.ToLower(t.Name)+g.opts.Suffix, typeTemplateName, map[string]any{
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
