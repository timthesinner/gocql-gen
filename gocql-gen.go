package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"strings"
)

var (
	model = flag.String("model", "", "Input model type, must have a corresponding .json file")
	dao   = flag.String("dao", "", "DAO type; resulting output file srcdir/<dao>_gen.go")
)

// Usage is a replacement usage function for the flags package.
func Usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "\tgocql-gen [flags] -model T -dao S\n")
	fmt.Fprintf(os.Stderr, "For more information, see:\n")
	fmt.Fprintf(os.Stderr, "\thttps://github.com/timthesinner/gocql-gen\n")
	fmt.Fprintf(os.Stderr, "Flags:\n")
	flag.PrintDefaults()
}

type columnDef struct {
	Name                string `json:"name"`
	CqlType             string `json:"type"`
	Key                 string `json:"key"`
	DeserializeFromBlob string `json:"deserializeTo"`
}

func (c *columnDef) String() string {
	return fmt.Sprintf("{Name:%v,Type:%v,Key:%v}", c.Name, c.CqlType, c.Key)
}

func main() {
	log.SetFlags(0)
	log.SetPrefix("gocql-gen: ")
	flag.Usage = Usage
	flag.Parse()

	if len(*model) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	if len(*dao) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	if m, err := os.Open(*model + ".json"); err != nil {
		log.Fatal(err)
	} else {
		var columns []*columnDef
		json.NewDecoder(m).Decode(&columns)
		if len(columns) == 0 {
			log.Fatalf("The %v column definition was empty.", *model)
			os.Exit(1)
		}

		model := _DAOModel{Package: "TBD", AdditionalImports: "TBD", Model: *model, DAO: *dao}

		for _, col := range columns {
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
			case "list<blob>":
				column.GoType = "[][]byte"
				column.SerializedType = col.DeserializeFromBlob
			case "map<string,blob>":
				column.GoType = "map[string][]byte"
				column.SerializedType = col.DeserializeFromBlob
			default:
				column.GoType = "UNKNOWN"
			}
			model.Columns = append(model.Columns, column)
		}

		if template, err := template.New("DaoTemplate").Parse(_DAOTemplate); err != nil {
			log.Fatalf("DAOTemplate was not legal: %v", err)
			os.Exit(1)
		} else if err := template.Execute(os.Stdout, model); err != nil {
			log.Fatalf("Error executing template: %v", err)
		}
	}
}

type param struct {
	Name           string
	GoType         string
	CqlType        string
	SerializedType string
}

type _DAOModel struct {
	Package           string
	AdditionalImports string
	Model             string
	ModelImport       string
	DAO               string

	Keyspace string
	Table    string
	Columns  []*param

	partitioningKeys []string
	clusteringKeys   []string
	clusteringOrder  []string
	keys             []string
}

func (m _DAOModel) Arguments() template.HTML {
	return template.HTML(strings.Join(os.Args[1:], " "))
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
		} else if c.CqlType == "map<string,blob>" {
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
			} else if c.CqlType == "map<string,blob>" {
				deser = append(deser, fmt.Sprintf(`
    for k, v := range %v {
      var value %v
      json.Unmarshal(v, &value)
      resource.%v[k] = value
    }`, c.Name, c.SerializedType, c.Name))
			}
		}
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
  for _, v := range resource.%v {
    if value, err := json.Marshal(v); err == nil {
      %v = append(%v, value)
    } else {
      fmt.Println("Could not marshal value:", err, v)
    }
  }`, c.Name, c.Name, c.Name, c.Name))
			} else if c.CqlType == "map<string,blob>" {
				ser = append(ser, fmt.Sprintf(`
  %v := make(map[string][]byte)
  for k, v := range resource.%v {
    if value, err := json.Marshal(v); err == nil {
      %v[k] = value
    } else {
      fmt.Println("Could not marshal attribute:", k, err, v)
    }
  }`, c.Name, c.Name, c.Name))
			}
		}
	}
	return template.HTML(strings.Join(ser, "\n") + "\n")
}

const _DAOTemplate = `// Code generated by "gocql-gen {{.Arguments}}"; DO NOT EDIT THIS FILE
/*
 *
 * Model that generated this code: {{.RawJSON}}
 *
 */
package {{.Package}}

import (
  "encoding/json"
  "fmt"
  "net/http"
  "os"
  "path"
  "strconv"
  "time"

  "github.com/gocql/gocql"

  {{.AdditionalImports}}
)

type {{.Model}}Stream struct {
  DTO *{{.ModelImport}}{{.Model}}
  ERR error
}

func (dao *{{.DAO}}) Init(session *gocql.Session) (error) {
  return session.Query(` + "`" + `CREATE TABLE IF NOT EXISTS {{.Keyspace}}.{{.Table}} (
{{.TableDefinition}},

    PRIMARY KEY ({{.PartitioningKeys}}{{.ClusteringColumns}})
  ){{.ClusteringOrder}};` + "`" + `).Exec()
}

func (dao *{{.DAO}}) Get({{.SelectSingleKeys}} interface{}, _session ...*gocql.Session) (*{{.ModelImport}}{{.Model}}, error) {
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

func (dao *{{.DAO}}) List({{.SelectListKeys}} interface{}, _session ...*gocql.Session) ([]*{{.ModelImport}}{{.Model}}, error) {
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

func (dao *{{.DAO}}) session(_session ...*gocql.Session) (*gocql.Session, error, bool) {
  if _session == nil || len(_session) != 1 || _session[0] == nil {
    if session, err := dao.CreateSession(); err != nil {
      return nil, err, false
    } else {
      return session, nil, true
    }
  }
  return _session[0], nil, false
}

func (dao *{{.DAO}}) add(r *{{.ModelImport}}{{.Model}}, session *gocql.Session) (*{{.ModelImport}}{{.Model}}, error) { {{.SerializeParameters}}
  err := session.Query(` + "`" + `INSERT INTO {{.Keyspace}}.{{.Table}} ({{.InsertFields}})
                      VALUES ({{.InsertValues}});` + "`" + `,
                      {{.InsertResource}}).Exec()
  if err != nil {
    return nil, err
  }
  return r, nil
}

func (dao *{{.DAO}}) stream(cql string, params ...interface{}) chan *{{.Model}}Stream {
  stream := make(chan *{{.Model}}Stream, dao.capacity())

  go func() {
    defer close(stream)

    if session, err := dao.CreateSession(); err != nil {
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
        resource := &{{.ModelImport}}{{.Model}}{
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

func (dao *{{.DAO}}) list(session *gocql.Session, cql string, params ...interface{}) ([]*{{.ModelImport}}{{.Model}}, error) {
  var (
    {{range .Columns}}{{.Name}} {{.GoType}}
    {{end}})

  iter := session.Query(cql, params...).Iter()
  results := make([]*{{.ModelImport}}{{.Model}}, dao.capacity())
  for iter.Scan({{.GetScanParameters}}) {
    resource := &{{.ModelImport}}{{.Model}}{
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

`
