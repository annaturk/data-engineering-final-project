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
type CCVIJsonRecords []struct {
	Geography_type			string `json:"geography_type"`
	Community_area_or_zip	string `json:"community_area_or_zip"`
	Community_area_name		string `json:"community_area_name"`
	CCVI_score				string `json:"ccvi_score"`
	CCVI_category			string `json:"ccvi_category"`
}

func GetCCVIData(db *sql.DB) {

	// Data Collection from data sources:
	// https://data.cityofchicago.org/Health-Human-Services/Chicago-COVID-19-Community-Vulnerability-Index-CCV/xhc6-88s9/about_data

	// Check if it has been a week since the last update

	fmt.Println("GetCCVI: Collecting Chicago COVID-19 Community Vulnerability Index Data")

	drop_table := `DROP TABLE IF EXISTS ccvi_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "ccvi_data" (
						"id"   SERIAL , 
						"geography_type" VARCHAR(255), 
						"community_area_or_zip" VARCHAR(255), 
						"community_area_name" VARCHAR(255), 
						"zip_code" VARCHAR(255), 
						"ccvi_score" FLOAT, 
						"ccvi_category" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var ccvi_url = "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=100"

	res, err := http.Get(ccvi_url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var ccvi_data_list CCVIJsonRecords
	json.Unmarshal(body, &ccvi_data_list)

	for i := 0; i < len(ccvi_data_list); i++ {
		// We will execute definsive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		geographyType := ccvi_data_list[i].Geography_type
		communityAreaOrZip := ccvi_data_list[i].Community_area_or_zip
		communityAreaName := ccvi_data_list[i].Community_area_name
		ccviScore := ccvi_data_list[i].CCVI_score
		ccviCategory := ccvi_data_list[i].CCVI_category

		// Skip records with missing or empty values
		if geographyType == "" || communityAreaOrZip == "" || ccviScore == "" || ccviCategory == "" {
			continue
		}

		var zipCode sql.NullString
		if geographyType == "ZIP" {
			zipCode.String = communityAreaOrZip
			zipCode.Valid = true
			communityAreaName = "" // Set to empty string to allow it to be NULL in the database
		}

		_, err := db.Exec(`
			INSERT INTO ccvi_data ("geography_type", "community_area_or_zip", "community_area_name", "zip_code", "ccvi_score", "ccvi_category") 
			VALUES ($1, $2, $3, $4, $5, $6)
    	`, geographyType, communityAreaOrZip, communityAreaName, zipCode, ccviScore, ccviCategory)

		if err != nil {
			panic(err)
		}

	}


}
