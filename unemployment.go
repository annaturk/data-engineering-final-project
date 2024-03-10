package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"database/sql"
	"encoding/json"

	_ "github.com/lib/pq"
)

type UnemploymentJsonRecords []struct {
	Community_area			string `json:"community_area"`
	Community_area_name		string `json:"community_area_name"`
	Poverty_level			string `json:"below_poverty_level"`
	Per_capita_income		string `json:"per_capita_income"`
	Unemployment			string `json:"unemployment"`
}

func GetUnemploymentRates(db *sql.DB) {

	// Data Collection from data sources:
	// https://data.cityofchicago.org/Health-Human-Services/Public-Health-Statistics-Selected-public-health-in/iqnk-2tcu/data
	
	fmt.Println("GetUnemploymentRates: Collecting Unemployment Rates Data")

	drop_table := `DROP TABLE IF EXISTS unemployment_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "unemployment_data" (
						"id"   SERIAL , 
						"community_area" VARCHAR(255), 
						"community_area_name" VARCHAR(255), 
						"poverty_level" FLOAT, 
						"per_capita_income" FLOAT, 
						"unemployment" FLOAT,
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var unemployment_url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=100"

	res, err := http.Get(unemployment_url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_data_list UnemploymentJsonRecords
	json.Unmarshal(body, &unemployment_data_list)

	for i := 0; i < len(unemployment_data_list); i++ {

		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		community_area := unemployment_data_list[i].Community_area
		if community_area == "" {
			continue
		}

		community_area_name := unemployment_data_list[i].Community_area_name
		if community_area_name == "" {
			continue
		}

		poverty_level := unemployment_data_list[i].Poverty_level
		if poverty_level == "" {
			continue
		}

		per_capita_income := unemployment_data_list[i].Per_capita_income
		if per_capita_income == "" {
			continue
		}

		unemployment := unemployment_data_list[i].Unemployment
		if unemployment == "" {
			continue
		}

		sql := `INSERT INTO unemployment_data ("community_area", "community_area_name", "poverty_level", "per_capita_income", "unemployment") values($1, $2, $3, $4, $5)`

		_, err = db.Exec(
			sql,
			community_area,
			community_area_name,
			poverty_level,
			per_capita_income,
			unemployment,
		)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("GetUnemploymentRates: Implement Unemployment")

}