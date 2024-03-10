package main

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// The following is a sample record from the Taxi Trips dataset retrieved from the City of Chicago Data Portal

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

// trip_id	"c354c843908537bbf90997917b714f1c63723785"
// trip_start_timestamp	"2021-11-13T22:45:00.000"
// trip_end_timestamp	"2021-11-13T23:00:00.000"
// trip_seconds	"703"
// trip_miles	"6.83"
// pickup_census_tract	"17031840300"
// dropoff_census_tract	"17031081800"
// pickup_community_area	"59"
// dropoff_community_area	"8"
// fare	"27.5"
// tip	"0"
// additional_charges	"1.02"
// trip_total	"28.52"
// shared_trip_authorized	false
// trips_pooled	"1"
// pickup_centroid_latitude	"41.8335178865"
// pickup_centroid_longitude	"-87.6813558293"
// pickup_centroid_location
// type	"Point"
// coordinates
// 		0	-87.6813558293
// 		1	41.8335178865
// dropoff_centroid_latitude	"41.8932163595"
// dropoff_centroid_longitude	"-87.6378442095"
// dropoff_centroid_location
// type	"Point"
// coordinates
// 		0	-87.6378442095
// 		1	41.8932163595
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"database/sql"
	"encoding/json"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

func GetShareTrips(db *sql.DB) {

	// Data Collection from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	
	fmt.Println("GetShareTrips: Collecting Share Trips Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	geocoder.ApiKey = "AIzaSyBF-uIJ5H6BAnCv4frQbw8xjVRf9tGjJQs"

 
	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var taxi_url = "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=100"

	res, err := http.Get(taxi_url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list TaxiTripsJsonRecords
	json.Unmarshal(body, &taxi_trips_list)


	for i := 0; i < len(taxi_trips_list); i++ {

		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := taxi_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := taxi_trips_list[i].Pickup_centroid_longitude
		//pickup_centroid_longitude := taxi_trips_list[i].PICKUP_LONG

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude
		//dropoff_centroid_latitude := taxi_trips_list[i].DROPOFF_LAT

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude
		//dropoff_centroid_longitude := taxi_trips_list[i].DROPOFF_LONG

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		pickup_address := pickup_address_list[0]
		pickup_zip_code := pickup_address.PostalCode

		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}

		// After getting the zipcode from geocoder we can now do the insert statement
		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)
		dropoff_address := dropoff_address_list[0]
		dropoff_zip_code := dropoff_address.PostalCode

		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code)

		if err != nil {
			panic(err)
		}

	}

}