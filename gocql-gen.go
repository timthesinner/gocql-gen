package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
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
		fmt.Println(columns)
	}
}
