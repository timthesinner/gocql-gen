package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"go/format"
)

// Usage is a replacement usage function for the flags package.
func Usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\tgocql-gen\n")
	fmt.Fprintf(os.Stderr, "For more information, see:\n")
	fmt.Fprintf(os.Stderr, "\thttps://github.com/timthesinner/gocql-gen\n")
}

type tableDef struct {
	Model         string       `json:"modelName"`
	Table         string       `json:"tableName"`
	DAO           string       `json:"dao"`
	GeneratedName string       `json:"generatedName"`
	Columns       []*columnDef `json:"columns"`
}

type columnDef struct {
	Name                string `json:"name"`
	CqlType             string `json:"type"`
	Key                 string `json:"key"`
	DeserializeFromBlob string `json:"deserializeTo"`
}

type modelDef struct {
	Package  string `json:"Package"`
	Location string `json:"Location"`
}

type persistDef struct {
	Keyspace          string      `json:"keyspace"`
	Package           string      `json:"package"`
	BoilerPlate       string      `json:"boilerplate"`
	AdditionalImports []string    `json:"imports"`
	ModelImport       string      `json:"modelPackage"`
	ModelGeneration   *modelDef   `json:"ModelGeneration"`
	Tables            []*tableDef `json:"tables"`
}

var COLLECTION_REGEX = regexp.MustCompile(`list<(.*)>|set<(.*)>`)

func (c *columnDef) String() string {
	return fmt.Sprintf("{Name:%v,Type:%v,Key:%v}", c.Name, c.CqlType, c.Key)
}

func open(file string) (*os.File, error) {
	f, err := os.Open(file)
	if err != nil {
		return os.Open(path.Join("config", file))
	}
	return f, nil
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("gocql-gen: ")
	flag.Usage = Usage
	flag.Parse()

	var persist *persistDef
	if p, err := open("persist-config.json"); err != nil {
		log.Fatal(err)
	} else if err := json.NewDecoder(p).Decode(&persist); err != nil {
		log.Fatal(err)
	} else if len(persist.Tables) == 0 {
		log.Fatalf("At least one table must be defined")
	} else {
		for _, table_def := range persist.Tables {
			if len(table_def.Columns) == 0 {
				log.Fatalf("Table %v had no columns defined", table_def.Table)
			}

			model := _DAOModel{
				Keyspace:          persist.Keyspace,
				Package:           persist.Package,
				BoilerPlate:       persist.BoilerPlate,
				AdditionalImports: persist.AdditionalImports,
				ModelImport:       persist.ModelImport,
				Model:             table_def.Model,
				Table:             table_def.Table,
				DAO:               table_def.DAO,
				IncludeTime:       false,
			}

			for _, col := range table_def.Columns {
				//model.Columns = append(model.Columns, col.Name+" "+col.CqlType)
				switch col.Key {
				case "partition":
					model.partitioningKeys = append(model.partitioningKeys, col.Name)
					model.keys = append(model.keys, col.Name)
				case "cluster", "cluster-asc", "cluster-desc":
					model.clusteringKeys = append(model.clusteringKeys, col.Name)
					model.keys = append(model.keys, col.Name)
				}

				switch col.Key {
				case "cluster-asc":
					model.clusteringOrder = append(model.clusteringOrder, col.Name+" ASC")
				case "cluster-desc":
					model.clusteringOrder = append(model.clusteringOrder, col.Name+" DESC")
				}

				column := &param{Name: col.Name, CqlType: col.CqlType}
				switch col.CqlType {
				case "text":
					column.GoType = "string"
				case "uuid", "timeuuid":
					column.GoType = "*gocql.UUID"
				case "int":
					column.GoType = "int"
				case "double":
					column.GoType = "float64"
				case "timestamp":
					column.GoType = "*time.Time"
					model.IncludeTime = true
				case "list<blob>":
					column.GoType = "[][]byte"
					column.SerializedType = col.DeserializeFromBlob
					if column.SerializedType != "" {
						model.IncludeJson = true
					}
				case "map<text,blob>":
					column.GoType = "map[string][]byte"
					column.SerializedType = col.DeserializeFromBlob
					if column.SerializedType != "" {
						model.IncludeJson = true
					}
				default:
					if match := COLLECTION_REGEX.FindStringSubmatch(col.CqlType); len(match) == 3 {
						t := match[1]
						if t == "" {
							t = match[2]
						}
						switch t {
						case "text":
							column.GoType = "[]string"
						case "uuid", "timeuuid":
							column.GoType = "[]*gocql.UUID"
						case "timestamp":
							column.GoType = "[]time.Time"
							model.IncludeTime = true
						case "int":
							column.GoType = "[]int"
						case "double":
							column.GoType = "[]float64"
						case "blob":
							column.GoType = "[][]byte"
						}
					}
				}
				model.Columns = append(model.Columns, column)
			}

			var result bytes.Buffer
			if template, err := template.New("DaoTemplate").Parse(_DAOTemplate); err != nil {
				log.Fatalf("DAOTemplate was not legal: %v", err)
			} else if err := template.Execute(&result, model); err != nil {
				log.Fatalf("Error executing template for %v: %v", table_def.Table, err)
			} else if res, err := format.Source(result.Bytes()); err != nil {
				log.Fatalf("Error formatting template for %v: %v\n%v", table_def.Table, err, string(result.Bytes()))
			} else if dao, err := os.Create(strings.ToLower(fmt.Sprintf("%v-dao_gen.go", table_def.GeneratedName))); err != nil {
				log.Fatalf("Could not create dao_gen source file: %v", err)
			} else if i, err := dao.Write(res); err != nil {
				log.Fatalf("Error writing template for %v: %v", table_def.Table, err)
			} else if i != len(res) {
				log.Fatalf("Did not write all template bytes for %v", table_def.Table)
			} else if persist.ModelGeneration != nil {
				model.Package = persist.ModelGeneration.Package
				var modelResult bytes.Buffer
				if mTemplate, err := template.New("ModelTemplate").Parse(_DTOTemplate); err != nil {
					log.Fatalf("DTOTemplate was not legal: %v", err)
				} else if err := mTemplate.Execute(&modelResult, model); err != nil {
					log.Fatalf("Error executing dto template for %v: %v", table_def.Model, err)
				} else if res, err := format.Source(modelResult.Bytes()); err != nil {
					log.Fatalf("Error formatting dto template for %v: %v\n%v", table_def.Table, err, string(modelResult.Bytes()))
				} else if dto, err := os.Create(strings.ToLower(path.Join(persist.ModelGeneration.Location, fmt.Sprintf("%v-dto_gen.go", table_def.GeneratedName)))); err != nil {
					log.Fatalf("Could not create dto_gen source file: %v", err)
				} else if i, err := dto.Write(res); err != nil {
					log.Fatalf("Error writing dto template for %v: %v", table_def.Table, err)
				} else if i != len(res) {
					log.Fatalf("Did not write all dao template bytes for %v", table_def.Table)
				}
			}
		}
	}
}

type param struct {
	Name           string
	GoType         string
	CqlType        string
	SerializedType string `json:"SerializedType,omitempty"`
}

type _DAOModel struct {
	Package           string
	AdditionalImports []string
	IncludeTime       bool
	IncludeJson       bool
	Model             string
	ModelImport       string
	DAO               string
	BoilerPlate       string

	Keyspace string
	Table    string
	Columns  []*param

	partitioningKeys []string
	clusteringKeys   []string
	clusteringOrder  []string
	keys             []string
}

func (m _DAOModel) InjectBoilerPlate() template.HTML {
	if m.BoilerPlate == "" {
		return template.HTML("")
	} else if _, err := os.Stat(m.BoilerPlate); os.IsNotExist(err) {
		log.Fatalf("Boiler plate template did not exist for %v: %v", m.Table, err)
	}

	var buff bytes.Buffer
	if t, err := template.ParseFiles(m.BoilerPlate); err != nil {
		log.Fatalf("Could not parse boiler plate for %v: %v", m.Table, err)
	} else if err := t.Execute(&buff, m); err != nil {
		log.Fatalf("Could not execute boiler plate template for %v: %v", m.Table, err)
	}
	return template.HTML(string(buff.Bytes()))
}

func (m _DAOModel) BaseImports() template.HTML {
	res := []string{`"fmt"`}
	if m.IncludeTime {
		res = append(res, `"time"`)
	}

	if m.IncludeJson {
		res = append(res, `"encoding/json"`)
	}
	return template.HTML(strings.Join(res, "\n"))
}

func (m _DAOModel) CleanAdditionalImports() template.HTML {
	res := make([]string, len(m.AdditionalImports))
	for i, im := range m.AdditionalImports {
		res[i] = "  " + im
	}
	return template.HTML(strings.Join(res, "\n"))
}

func (m _DAOModel) ModelType() template.HTML {
	if m.ModelImport == "" {
		return template.HTML(m.Model)
	}
	return template.HTML(m.ModelImport + "." + m.Model)
}

func (m _DAOModel) TableDefinition() template.HTML {
	params := make([]string, len(m.Columns))
	for i, p := range m.Columns {
		params[i] = fmt.Sprintf("    %v %v", p.Name, p.CqlType)
	}
	return template.HTML(strings.Join(params, ",\n"))
}

func (m _DAOModel) PartitioningKeys() template.HTML {
	if len(m.partitioningKeys) == 0 {
		log.Fatal("Partitioning keys were empty")
		os.Exit(1)
	} else if len(m.partitioningKeys) == 1 {
		return template.HTML(m.partitioningKeys[0])
	}
	return template.HTML(fmt.Sprintf("(%v)", strings.Join(m.partitioningKeys, ", ")))
}

func (m _DAOModel) ClusteringColumns() template.HTML {
	if len(m.clusteringKeys) == 0 {
		return template.HTML("")
	}
	return template.HTML(fmt.Sprintf(", %v", strings.Join(m.clusteringKeys, ", ")))
}

func (m _DAOModel) ClusteringOrder() template.HTML {
	if len(m.clusteringOrder) == 0 {
		return template.HTML("")
	}
	return template.HTML(fmt.Sprintf(" WITH CLUSTERING ORDER BY (%v)", strings.Join(m.clusteringOrder, ", ")))
}

func (m _DAOModel) GetScanParameters() template.HTML {
	params := make([]string, len(m.Columns))
	for i, p := range m.Columns {
		params[i] = "&" + p.Name
	}
	return template.HTML(strings.Join(params, ", "))
}

func (m _DAOModel) RawJSON() template.HTML {
	raw, _ := json.MarshalIndent(&m, " * ", "  ")
	return template.HTML(string(raw))
}

func (m _DAOModel) EmitStream() template.HTML {
	return template.HTML(fmt.Sprintf("stream <- &%vStream", m.Model))
}

func (m _DAOModel) InsertFields() template.HTML {
	params := make([]string, len(m.Columns))
	for i, p := range m.Columns {
		params[i] = p.Name
	}
	return template.HTML(strings.Join(params, ", "))
}

func (m _DAOModel) InsertValues() template.HTML {
	params := make([]string, len(m.Columns))
	for i := range m.Columns {
		params[i] = "?"
	}
	return template.HTML(strings.Join(params, ", "))
}

func (m _DAOModel) InsertResource() template.HTML {
	params := make([]string, len(m.Columns))
	for i, p := range m.Columns {
		if p.SerializedType == "" {
			params[i] = "r." + p.Name
		} else {
			params[i] = p.Name
		}
	}
	return template.HTML(strings.Join(params, ", "))
}

func (m _DAOModel) SelectSingleKeys() template.HTML {
	return template.HTML(strings.Join(m.keys, ", "))
}

func (m _DAOModel) DeleteKeys() template.HTML {
	keys := make([]string, len(m.keys))
	for i, k := range m.keys {
		keys[i] = "r." + k
	}
	return template.HTML(strings.Join(keys, ", "))
}

func (m _DAOModel) SelectSingle() template.HTML {
	keys := make([]string, len(m.keys))
	for i, k := range m.keys {
		keys[i] = k + "=?"
	}
	return template.HTML(strings.Join(keys, " AND "))
}

func (m _DAOModel) SelectListKeys() template.HTML {
	return template.HTML(strings.Join(m.partitioningKeys, ", "))
}

func (m _DAOModel) SelectList() template.HTML {
	keys := make([]string, len(m.partitioningKeys))
	for i, k := range m.partitioningKeys {
		keys[i] = k + "=?"
	}
	return template.HTML(strings.Join(keys, " AND "))
}

func (m _DAOModel) CreateResourceFromParameters() template.HTML {
	resource := make([]string, len(m.Columns))
	for i, c := range m.Columns {
		if c.SerializedType == "" {
			resource[i] = fmt.Sprintf("          %v: %v", c.Name, c.Name)
		} else if c.CqlType == "list<blob>" {
			resource[i] = fmt.Sprintf("          %v: make([]%v, 0)", c.Name, c.SerializedType)
		} else if c.CqlType == "map<text,blob>" {
			resource[i] = fmt.Sprintf("          %v: make(map[string]%v)", c.Name, c.SerializedType)
		}
	}
	return template.HTML(strings.Join(resource, ",\n") + ",")
}

func (m _DAOModel) DeserializeParameters() template.HTML {
	deser := make([]string, 0)
	for _, c := range m.Columns {
		if c.SerializedType != "" {
			if c.CqlType == "list<blob>" {
				deser = append(deser, fmt.Sprintf(`
    for _, v := range %v {
      var value %v
      json.Unmarshal(v, &value)
      resource.%v = append(resource.%v, value)
    }`, c.Name, c.SerializedType, c.Name, c.Name))
			} else if c.CqlType == "map<text,blob>" {
				deser = append(deser, fmt.Sprintf(`
    for k, v := range %v {
      var value %v
      json.Unmarshal(v, &value)
      resource.%v[k] = value
    }`, c.Name, c.SerializedType, c.Name))
			}
		}
	}

	if len(deser) == 0 {
		return template.HTML("")
	}

	return template.HTML(strings.Join(deser, "\n"))
}

func (m _DAOModel) SerializeParameters() template.HTML {
	ser := make([]string, 0)
	for _, c := range m.Columns {
		if c.SerializedType != "" {
			if c.CqlType == "list<blob>" {
				ser = append(ser, fmt.Sprintf(`
  %v := make([][]byte, 0)
  for _, v := range r.%v {
    if value, err := json.Marshal(v); err == nil {
      %v = append(%v, value)
    } else {
      fmt.Println("Could not marshal value:", err, v)
    }
  }`, c.Name, c.Name, c.Name, c.Name))
			} else if c.CqlType == "map<text,blob>" {
				ser = append(ser, fmt.Sprintf(`
  %v := make(map[string][]byte)
  for k, v := range r.%v {
    if value, err := json.Marshal(v); err == nil {
      %v[k] = value
    } else {
      fmt.Println("Could not marshal attribute:", k, err, v)
    }
  }`, c.Name, c.Name, c.Name))
			}
		}
	}

	if len(ser) == 0 {
		return template.HTML("")
	}

	return template.HTML(strings.Join(ser, "\n") + "\n")
}

func (m _DAOModel) BaseModelImports() template.HTML {
	if m.IncludeTime {
		return template.HTML(`"time"`)
	}
	return template.HTML("")
}

func (m _DAOModel) ModelFields() template.HTML {
	fields := make([]string, len(m.Columns))
	for i, c := range m.Columns {
		r, n := utf8.DecodeRuneInString(c.Name)
		jsonName := string(unicode.ToLower(r)) + c.Name[n:]

		if c.SerializedType == "" {
			fields[i] = fmt.Sprintf("%v %v `json:\"%v\"`", c.Name, c.GoType, jsonName)
		} else {
			t := c.SerializedType
			if strings.Contains(c.SerializedType, m.ModelImport+".") {
				t = strings.Replace(c.SerializedType, m.ModelImport+".", "", 1)
			}

			if c.CqlType == "list<blob>" {
				fields[i] = fmt.Sprintf("%v []%v `json:\"%v\"`", c.Name, t, jsonName)
			} else if c.CqlType == "map<text,blob>" {
				fields[i] = fmt.Sprintf("%v map[string]%v `json:\"%v\"`", c.Name, t, jsonName)
			}
		}
	}
	return template.HTML(strings.Join(fields, "\n"))
}

const _DAOTemplate = `// Code generated by "gocql-gen"; DO NOT EDIT THIS FILE
/*
 *
 * Model that generated this code: {{.RawJSON}}
 *
 */
package {{.Package}}

import (
{{.BaseImports}}

  "github.com/gocql/gocql"

{{.CleanAdditionalImports}}
)

{{.InjectBoilerPlate}}

type {{.Model}}Stream struct {
  DTO *{{.ModelType}}
  ERR error
}

func (dao *{{.DAO}}) Init(session *gocql.Session) (error) {
  return session.Query(` + "`" + `CREATE TABLE IF NOT EXISTS {{.Keyspace}}.{{.Table}} (
{{.TableDefinition}},

    PRIMARY KEY ({{.PartitioningKeys}}{{.ClusteringColumns}})
  ){{.ClusteringOrder}};` + "`" + `).Exec()
}

func (dao *{{.DAO}}) Add(r *{{.ModelType}}, session *gocql.Session) (*{{.ModelType}}, error) { {{.SerializeParameters}}
  err := session.Query(` + "`" + `INSERT INTO {{.Keyspace}}.{{.Table}} ({{.InsertFields}})
                      VALUES ({{.InsertValues}});` + "`" + `,
                      {{.InsertResource}}).Exec()
  if err != nil {
    return nil, err
  }
  return r, nil
}

func (dao *{{.DAO}}) Get({{.SelectSingleKeys}} interface{}, _session ...*gocql.Session) (*{{.ModelType}}, error) {
  session, err, close := dao.session(_session...)
  if err != nil {
    return nil, err
  } else if close {
    defer session.Close()
  }

  if res, err := dao.list(session, ` + "`" + `SELECT {{.InsertFields}} FROM {{.Keyspace}}.{{.Table}} WHERE {{.SelectSingle}};` + "`" + `, {{.SelectSingleKeys}}); err != nil {
    return nil, err
  } else if len(res) != 1 {
    return nil, nil
  } else {
    return res[0], nil
  }
}

func (dao *{{.DAO}}) List({{.SelectListKeys}} interface{}, _session ...*gocql.Session) ([]*{{.ModelType}}, error) {
  session, err, close := dao.session(_session...)
  if err != nil {
    return nil, err
  } else if close {
    defer session.Close()
  }

  return dao.list(session, ` + "`" + `SELECT {{.InsertFields}} FROM {{.Keyspace}}.{{.Table}} WHERE {{.SelectList}};` + "`" + `, {{.SelectListKeys}})
}

func (dao *{{.DAO}}) Stream({{.SelectListKeys}} interface{}) chan *{{.Model}}Stream {
  return dao.stream(` + "`" + `SELECT {{.InsertFields}} FROM {{.Keyspace}}.{{.Table}} WHERE {{.SelectList}};` + "`" + `, {{.SelectListKeys}})
}

func (dao *{{.DAO}}) Delete(r *{{.ModelType}}, _session ...*gocql.Session) error {
  session, err, close := dao.session(_session...)
  if err != nil {
    return err
  } else if close {
    defer session.Close()
  }

  return dao.delete(session, ` + "`" + `DELETE FROM {{.Keyspace}}.{{.Table}} WHERE {{.SelectSingle}};` + "`" + `, {{.DeleteKeys}})
}

func (dao *{{.DAO}}) session(_session ...*gocql.Session) (*gocql.Session, error, bool) {
  if _session == nil || len(_session) != 1 || _session[0] == nil {
    if session, err := dao.createSession(); err != nil {
      return nil, err, false
    } else {
      return session, nil, true
    }
  }
  return _session[0], nil, false
}

func (dao *{{.DAO}}) stream(cql string, params ...interface{}) chan *{{.Model}}Stream {
  stream := make(chan *{{.Model}}Stream, dao.capacity())

  go func() {
    defer close(stream)

    if session, err := dao.createSession(); err != nil {
      fmt.Println("Could not initialize sesion to stream resources for {{.Table}}", err)
      {{.EmitStream}}{DTO: nil, ERR: err}
    } else {
      defer session.Close()
      session.SetPageSize(dao.pageSize())

      var (
        {{range .Columns}}{{.Name}} {{.GoType}}
        {{end}})

      iter := session.Query(cql, params...).Iter()
      for iter.Scan({{.GetScanParameters}}) {
        resource := &{{.ModelType}}{
{{.CreateResourceFromParameters}}
        }
        {{.DeserializeParameters}}

        {{.EmitStream}}{DTO: resource, ERR: nil}
      }

      if err := iter.Close(); err != nil {
        fmt.Println("Error streaming resources for {{.Table}}", cql, err)
        {{.EmitStream}}{DTO: nil, ERR: err}
      }
    }
  }()

  return stream
}

func (dao *{{.DAO}}) list(session *gocql.Session, cql string, params ...interface{}) ([]*{{.ModelType}}, error) {
  var (
    {{range .Columns}}{{.Name}} {{.GoType}}
    {{end}})

  session.SetPageSize(dao.pageSize())
  iter := session.Query(cql, params...).Iter()
  results := make([]*{{.ModelType}}, dao.capacity())
  for iter.Scan({{.GetScanParameters}}) {
    resource := &{{.ModelType}}{
{{.CreateResourceFromParameters}}
    }
    {{.DeserializeParameters}}

    results = append(results, resource)
  }

  if err := iter.Close(); err != nil {
    fmt.Println("Error listing resources for {{.Table}}", cql, err)
    return nil, err
  }

  return results, nil
}

func (dao *{{.DAO}}) delete(session *gocql.Session, cql string, params ...interface{}) error {
  return session.Query(cql, params...).Exec()
}

`

const _DTOTemplate = `// Code generated by "gocql-gen"; DO NOT EDIT THIS FILE
/*
 *
 * Model that generated this code: {{.RawJSON}}
 *
 */
package {{.Package}}

import (
{{.BaseModelImports}}

  "github.com/gocql/gocql"
)

type {{.Model}} struct {
	{{.ModelFields}}
}

`
