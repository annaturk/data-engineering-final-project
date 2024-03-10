package main

import (
	"fmt"
	"io"
	"log"
	"strconv"
	"net/http"
	"database/sql"
	"encoding/json"
	
	_ "github.com/lib/pq"
)

// This is all we need from the full record
type COVIDCasesJsonRecord []struct {
	Zip_code       string    `json:"zip_code"`
	Week_number    string    `json:"week_number"`
	Cases_weekly   string    `json:"cases_weekly"`
	Population     string    `json:"population"`
}

func GetCOVIDCases(db *sql.DB) {
	fmt.Println("GetCOVIDCases: Collecting Covid Cases Data")

	drop_table := `DROP TABLE IF EXISTS covid_cases`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "covid_cases" (
						"id"   SERIAL , 
						"zip_code" VARCHAR(255), 
						"week_number" VARCHAR(255), 
						"cases_weekly" INT, 
						"population" INT,
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	var covid_url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=100"

	response, err := http.Get(covid_url)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}

	// Unmarshal JSON response into slice of CovidZip structs
	var rows COVIDCasesJsonRecord
	err = json.Unmarshal(body, &rows)
	if err != nil {
		panic(err)
	}

	// Insert data into database
	for _, row := range rows {
		// Convert Cases_weekly to integer
		casesWeeklyInt, err := strconv.Atoi(row.Cases_weekly)
		if err != nil {
			log.Println("Error converting cases_weekly to int:", err)
			continue
		}
	
		// Check if Population is empty, set to NULL if empty
		var populationInt sql.NullInt64
		if row.Population != "" {
			populationVal, err := strconv.Atoi(row.Population)
			if err != nil {
				log.Println("Error converting population to int:", err)
				continue
			}
			populationInt.Int64 = int64(populationVal)
			populationInt.Valid = true
		}
	
		// Insert data into database
		_, err = db.Exec(`
			INSERT INTO covid_cases (
				Zip_code, 
				Week_number, 
				Cases_weekly, 
				Population
			) VALUES ($1, $2, $3, $4)
		`, row.Zip_code, row.Week_number, casesWeeklyInt, populationInt)
		if err != nil {
			log.Fatal(err)
		}
	}
	
}
