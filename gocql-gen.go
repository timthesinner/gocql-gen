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
	Name string `json:"name"`
	Type string `json:"type"`
	Key  string `json:"key"`
}

func (c *columnDef) String() string {
	return fmt.Sprintf("{Name:%v,Type:%v,Key:%v}", c.Name, c.Type, c.Key)
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

	fmt.Println(*model)
	fmt.Println(*dao)

	if m, err := os.Open(*model + ".json"); err != nil {
		log.Fatal(err)
	} else {
		var columns []*columnDef
		json.NewDecoder(m).Decode(&columns)
		if len(columns) == 0 {
			log.Fatalf("The %v column definition was empty.", *model)
			os.Exit(1)
		}

		model := DAOModel{Arguments: strings.Join(os.Args[1:], " "), Package: "TBD", AdditionalImports: "TBD", Model: *model, DAO: *dao}

		clusteringColumns := []string{}
		clusteringOrder := []string{}
		for _, col := range columns {
			model.Columns = append(model.Columns, col.Name+" "+col.Type)
			switch col.Key {
			case "partition":
				model.partitioningKeys = append(model.partitioningKeys, col.Name)
				model.keys = append(model.keys, col.Name)
			case "cluster", "cluster-asc", "cluster-desc":
				clusteringColumns = append(clusteringColumns, col.Name)
				model.keys = append(model.keys, col.Name)
			}

			switch col.Key {
			case "cluster-asc":
				clusteringOrder = append(clusteringOrder, col.Name+" ASC")
			case "cluster-desc":
				clusteringOrder = append(clusteringOrder, col.Name+" DESC")
			}

			selectParam := &param{Name: col.Name}
			switch col.Type {
			case "text":
				selectParam.Type = "string"
			case "uuid", "timeuuid":
				selectParam.Type = "*gocql.UUID"
			default:
				selectParam.Type = "UNKNOWN"
			}
			model.SelectParameters = append(model.SelectParameters, selectParam)
		}

		model.InitPartitioningKeys()
		model.InitClusteringColumns(clusteringColumns)
		model.InitClusteringOrder(clusteringOrder)

		if template, err := template.New("DaoTemplate").Parse(DAOTemplate); err != nil {
			log.Fatalf("DAOTemplate was not legal: %v", err)
			os.Exit(1)
		} else if err := template.Execute(os.Stdout, model); err != nil {
			log.Fatalf("Error executing template: %v", err)
		}
	}
}

type param struct {
	Name string
	Type string
}

type DAOModel struct {
	Arguments         string
	Package           string
	AdditionalImports string
	Model             string
	ModelImport       string
	DAO               string

	Keyspace          string
	Table             string
	Columns           []string
	PartioningKeys    string
	ClusteringColumns string
	ClusteringOrder   string
	SelectParameters  []*param

	partitioningKeys []string
	keys             []string
}

func (m *DAOModel) InitPartitioningKeys() {
	if len(m.partitioningKeys) == 0 {
		log.Fatal("Partitioning keys were empty")
		os.Exit(1)
	} else if len(m.partitioningKeys) == 1 {
		m.PartioningKeys = m.partitioningKeys[0]
	} else {
		m.PartioningKeys = fmt.Sprintf("(%v)", strings.Join(m.partitioningKeys, ", "))
	}
}

func (m *DAOModel) InitClusteringColumns(keys []string) {
	if len(keys) == 0 {
		return
	}
	m.ClusteringColumns = fmt.Sprintf(", %v", strings.Join(keys, ", "))
}

func (m *DAOModel) InitClusteringOrder(keys []string) {
	if len(keys) == 0 {
		return
	}
	m.ClusteringOrder = fmt.Sprintf(" WITH CLUSTERING ORDER BY (%v)", strings.Join(keys, ", "))
}

func (m DAOModel) GetScanParameters() template.HTML {
	params := make([]string, len(m.SelectParameters))
	for i, p := range m.SelectParameters {
		params[i] = "&" + p.Name
	}
	return template.HTML(strings.Join(params, ", "))
}

func (m DAOModel) RawJSON() template.HTML {
	raw, _ := json.MarshalIndent(&m, " * ", "  ")
	return template.HTML(string(raw))
}

func (m DAOModel) EmitStream() template.HTML {
	return template.HTML(fmt.Sprintf("stream <- &%vStream", m.Model))
}

func (m DAOModel) InsertFields() template.HTML {
	params := make([]string, len(m.SelectParameters))
	for i, p := range m.SelectParameters {
		params[i] = p.Name
	}
	return template.HTML(strings.Join(params, ", "))
}

func (m DAOModel) InsertValues() template.HTML {
	params := make([]string, len(m.SelectParameters))
	for i, _ := range m.SelectParameters {
		params[i] = "?"
	}
	return template.HTML(strings.Join(params, ", "))
}

func (m DAOModel) InsertResource() template.HTML {
	params := make([]string, len(m.SelectParameters))
	for i, p := range m.SelectParameters {
		params[i] = "r." + p.Name
	}
	return template.HTML(strings.Join(params, ", "))
}

func (m DAOModel) GetAllKeys() template.HTML {
	return template.HTML(strings.Join(m.keys, ", "))
}

func (m DAOModel) SelectSingle() template.HTML {
	keys := make([]string, len(m.keys))
	for i, k := range m.keys {
		keys[i] = k + "=?"
	}
	return template.HTML(strings.Join(keys, " AND "))
}

func (m DAOModel) GetKeys() template.HTML {
	return template.HTML(strings.Join(m.partitioningKeys, ", "))
}

func (m DAOModel) SelectList() template.HTML {
	keys := make([]string, len(m.partitioningKeys))
	for i, k := range m.partitioningKeys {
		keys[i] = k + "=?"
	}
	return template.HTML(strings.Join(keys, " AND "))
}

const DAOTemplate = `
// Code generated by "gocql-gen {{.Arguments}}"; DO NOT EDIT THIS FILE
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
    {{range .Columns}}{{.}},
    {{end}}

    PRIMARY KEY ({{.PartioningKeys}}{{.ClusteringColumns}})
  ){{.ClusteringOrder}};` + "`" + `).Exec()
}

func (dao *{{.DAO}}) Get({{.GetAllKeys}} interface{}, _session ...*gocql.Session) (*{{.ModelImport}}{{.Model}}, error) {
  session, err, close := dao.session(_session...)
	if err != nil {
		return nil, err
	} else if close {
		defer session.Close()
	}

  if res, err := dao.list(session, ` + "`" + `SELECT {{.InsertFields}} FROM {{.Keyspace}}.{{.Table}} WHERE {{.SelectSingle}};` + "`" + `, {{.GetKeys}}); err != nil {
    return nil, err
  } else if len(res) != 1 {
    return nil, nil
  } else {
    return res[0], nil
  }
}

func (dao *{{.DAO}}) List({{.GetKeys}} interface{}, _session ...*gocql.Session) ([]*{{.ModelImport}}{{.Model}}, error) {
  session, err, close := dao.session(_session...)
	if err != nil {
		return nil, err
	} else if close {
		defer session.Close()
	}

  return dao.list(session, ` + "`" + `SELECT {{.InsertFields}} FROM {{.Keyspace}}.{{.Table}} WHERE {{.SelectList}};` + "`" + `, {{.GetKeys}})
}

func (dao *{{.DAO}}) Stream({{.GetKeys}} interface{}) chan *{{.Model}}Stream {
  return dao.stream(` + "`" + `SELECT {{.InsertFields}} FROM {{.Keyspace}}.{{.Table}} WHERE {{.SelectList}};` + "`" + `, {{.GetKeys}})
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

func (dao *{{.DAO}}) add(r *{{.ModelImport}}{{.Model}}, session *gocql.Session) (*{{.ModelImport}}{{.Model}}, error) {
  err := session.Query(` + "`" + `INSERT INTO {{.Keyspace}}.{{.Table}} ({{.InsertFields}})
                      VALUES ({{.InsertValues}});` + "`" + `,
                      {{.InsertResource}}).Exec()
  if err != nil {
    return nil, err
  }
  return r, nil
}

func (dao *{{.DAO}}) stream(cql string, params ...interface{}) chan *{{.Model}}Stream {
  stream := make(chan *{{.Model}}Stream, 4)

  go func() {
    defer close(stream)

    if session, err := dao.CreateSession(); err != nil {
      {{.EmitStream}}{DTO: nil, ERR: err}
    } else {
      defer session.Close()
      session.SetPageSize(dao.pageSize())

      var (
        {{range .SelectParameters}}{{.Name}} {{.Type}}
        {{end}})

      iter := session.Query(cql, params...).Iter()
      for iter.Scan({{.GetScanParameters}}) {
        resource := &{{.ModelImport}}{{.Model}}{
          {{range .SelectParameters}}{{.Name}}: {{.Name}},
          {{end}}}
        {{.EmitStream}}{DTO: resource, ERR: nil}
      }

      if err := iter.Close(); err != nil {
        {{.EmitStream}}{DTO: nil, ERR: err}
      }
    }
  }()

  return stream
}

func (dao *{{.DAO}}) list(session *gocql.Session, cql string, params ...interface{}) ([]*{{.ModelImport}}{{.Model}}, error) {
  var (
    {{range .SelectParameters}}{{.Name}} {{.Type}}
    {{end}})

  iter := session.Query(cql, params...).Iter()
  results := make([]*{{.ModelImport}}{{.Model}}, 4)
  for iter.Scan({{.GetScanParameters}}) {
    resource := &{{.ModelImport}}{{.Model}}{
      {{range .SelectParameters}}{{.Name}}: {{.Name}},
      {{end}}}
    results = append(results, resource)
  }

  if err := iter.Close(); err != nil {
    fmt.Println(err)
    return nil, err
  }

  return results, nil
}
`
