package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"database/sql"
	"encoding/json"

	_ "github.com/lib/pq"
)

// This is all we need from the full record 
type BoundarysonRecords []struct {
	Area_number			string `json:"area_numbe"`
	Community			string `json:"community"`
}

func GetNeighborhoodNames(db *sql.DB) {

	// Data Collection from data sources:
	// https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Community-Areas-current-/cauq-8yn6

	// Check if it has been a week since the last update

	fmt.Println("GetNeighborhoodNames: Collecting Neighborhood Names Data")

	drop_table := `DROP TABLE IF EXISTS boundary_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "boundary_data" (
						"id"   SERIAL , 
						"area_numbe" VARCHAR(255), 
						"community" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var boundary_url = "https://data.cityofchicago.org/resource/igwz-8jzy.json?$limit=100"

	res, err := http.Get(boundary_url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var neighborhood_name_list BoundarysonRecords
	json.Unmarshal(body, &neighborhood_name_list)

	for i := 0; i < len(neighborhood_name_list); i++ {
		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		areaNumber := neighborhood_name_list[i].Area_number
		community := neighborhood_name_list[i].Community

		// Skip records with missing or empty values
		if areaNumber == "" || community == "" {
			continue
		}

		_, err := db.Exec(`
			INSERT INTO boundary_data ("area_numbe", "community") 
			VALUES ($1, $2)
    	`, areaNumber, community)

		if err != nil {
			panic(err)
		}

	}


}