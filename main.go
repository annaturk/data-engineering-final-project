package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	// OPTION 1 - Postgress application running on localhost
	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=localhost sslmode=disable"

	// Establish connection to Postgres Database
	// db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=host.docker.internal sslmode=disable port=5433"

	db, err := sql.Open("postgres", db_connection)
	if err != nil {
		fmt.Println("Error connecting to database:", err)
		return
	}

	// Test the database connection
	err = db.Ping()
	if err != nil {
		fmt.Println("Couldn't connect to database:", err)
		return
	}

	// Continuous data collection loop
	for {
		GetTaxiTrips(db)
		GetShareTrips(db)
		GetCOVIDCases(db)
		GetCCVIData(db)
		GetBuildingPermits(db)
		GetUnemploymentRates(db)
		GetNeighborhoodNames(db)

		time.Sleep(24 * time.Hour) // Sleep for 24 hours before the next iteration
	}
}

