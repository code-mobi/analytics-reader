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
	"sync"
)

func main() {
	filePtr := flag.String("file", "analytics.json", "Json File Path")
	flag.Parse()

	db, err := sql.Open("mysql", os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	a := &AnalyticsReader{db: db}

	shows := a.getShow(*filePtr)
	for _, show := range shows {
		fmt.Println(show.Title, show.ViewCount)
		a.updateView(show)
	}
}

type AnalyticsReader struct {
	db *sql.DB
}

func (a *AnalyticsReader) getShow(file string) []*ShowItem {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}

	var analytics Analytics
	err = json.Unmarshal(b, &analytics)
	if err != nil {
		panic(err)
	}

	var shows []*ShowItem

	for _, rowCluster := range analytics.Components[0].DataTable.RowClusters {
		displayKey := rowCluster.RowKey[0].DisplayKey
		dataValue, err := strconv.Atoi(strings.Replace(rowCluster.Row[0].RowValue[0].DataValue, ",", "", -1))
		if err != nil {
			panic(err)
		}

		shows = append(shows, &ShowItem{displayKey, dataValue})
	}
	return shows
}

func (a *AnalyticsReader) getShowAsync(file string) chan *ShowItem {
	b, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}

	var analytics Analytics
	err = json.Unmarshal(b, &analytics)
	if err != nil {
		panic(err)
	}

	shows := make(chan *ShowItem)
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(analytics.Components[0].DataTable.RowClusters))

	for _, rowCluster := range analytics.Components[0].DataTable.RowClusters {
		go func(rowCluster *RowCluster) {
			displayKey := rowCluster.RowKey[0].DisplayKey
			dataValue, err := strconv.Atoi(strings.Replace(rowCluster.Row[0].RowValue[0].DataValue, ",", "", -1))
			if err != nil {
				panic(err)
			}

			shows <- &ShowItem{displayKey, dataValue}
			waitGroup.Done()
		}(rowCluster)
	}

	go func() {
		waitGroup.Wait()

		close(shows)
	}()

	return shows
}

func (a *AnalyticsReader) updateView(show *ShowItem) {
	_, err := a.db.Exec("UPDATE tv_program SET view_count = ? WHERE program_title = ?", show.ViewCount, show.Title)
	if err != nil {
		panic(err)
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

type ShowItem struct {
	Title     string
	ViewCount int
}

type Result struct {
	Title       string
	RowAffected int64
}
