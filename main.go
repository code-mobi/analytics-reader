package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

func main() {
	filePtr := flag.String("file", "analytics.json", "Json File Path, Default tophits.json")
	flag.Parse()

	fmt.Println("Start")
	b, err := ioutil.ReadFile(*filePtr)
	if err != nil {
		panic(err)
	}

	// fmt.Println(string(b))

	var analytics Analytics
	err = json.Unmarshal(b, &analytics)
	if err != nil {
		panic(err)
	}

	db, err := sql.Open("mysql", os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	for _, rowCluster := range analytics.Components[0].DataTable.RowClusters {
		displayKey := rowCluster.RowKey[0].DisplayKey
		dataValue, err := strconv.Atoi(strings.Replace(rowCluster.Row[0].RowValue[0].DataValue, ",", "", -1))
		if err != nil {
			panic(err)
		}

		result, err := db.Exec("UPDATE tv_program SET view_count = ? WHERE program_title = ?", dataValue, displayKey)
		if err != nil {
			panic(err)
		}
		fmt.Println(displayKey, dataValue, result)
	}

}

type Analytics struct {
	Components []*Components `json:"components"`
}

type Components struct {
	DataTable DataTable `json:"dataTable"`
}

type DataTable struct {
	RowClusters []*RowCluster `json:"rowCluster"`
}

type RowCluster struct {
	RowKey []*RowKey `json:"rowKey"`
	Row    []*Row    `json:"row"`
}

type RowKey struct {
	DisplayKey string `json:"displayKey"`
}

type Row struct {
	RowValue []*RowValue `json:"rowValue"`
}

type RowValue struct {
	DataValue string `json:"dataValue"`
}
