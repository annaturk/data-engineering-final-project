package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	_ "github.com/lib/pq"
)

// This is all we need from the full record
type BuildingPermitsJsonRecords []struct {
	Permit_number        string `json:"permit_"`
	Permit_type          string `json:"permit_type"`
	Review_type          string `json:"review_type"`
	Application_start_date string `json:"application_start_date"`
	Issue_date           string `json:"issue_date"`
	Community_area       string `json:"community_area"`
	Xcoordinate          string `json:"xcoordinate"`
	Ycoordinate          string `json:"ycoordinate"`
}

func IsValidDateFormat(date string) bool {
	// Since the format of dates in JSON data might vary, we skip the format check for now
	return true
}

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")

	drop_table := `DROP TABLE IF EXISTS building_permits`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permits" (
		"permit_number" VARCHAR(255), 
		"permit_type" VARCHAR(255), 
		"review_type" VARCHAR(255), 
		"application_start_date" DATE, 
		"issue_date" DATE, 
		"community_area" VARCHAR(255), 
		"xcoordinate" DOUBLE PRECISION, 
		"ycoordinate" DOUBLE PRECISION 
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	var permit_url = "https://data.cityofchicago.org/resource/ydr8-5enu.json?$limit=100"

	res, err := http.Get(permit_url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var building_permit_list BuildingPermitsJsonRecords
	json.Unmarshal(body, &building_permit_list)

	for i := 0; i < len(building_permit_list); i++ {

		permit_number := building_permit_list[i].Permit_number
		if permit_number == "" {
			continue
		}

		permit_type := building_permit_list[i].Permit_type
		if permit_type == "" {
			continue
		}

		review_type := building_permit_list[i].Review_type
		if review_type == "" {
			continue
		}

		application_start_date := building_permit_list[i].Application_start_date

		issue_date := building_permit_list[i].Issue_date

		community_area := building_permit_list[i].Community_area
		if community_area == "" {
			continue
		}

		xcoordinate := building_permit_list[i].Xcoordinate

		if xcoordinate == "" {
			continue
		}

		ycoordinate := building_permit_list[i].Ycoordinate

		if ycoordinate == "" {
			continue
		}

		sql := `INSERT INTO building_permits ("permit_number", "permit_type", "review_type", "application_start_date", "issue_date", "community_area", "xcoordinate", "ycoordinate") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

		_, err = db.Exec(
			sql,
			permit_number,
			permit_type,
			review_type,
			application_start_date,
			issue_date,
			community_area,
			xcoordinate,
			ycoordinate,
		)

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("GetBuildingPermits: Implement Building Permits")
}
