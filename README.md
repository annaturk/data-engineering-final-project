List of Program/Microservices Implemented:
- building_permits.go
- ccvi.go
- covid_cases.go
- share_trips.go
- taxi_trips.go
- unemployment.go 

Steps to Install and Run: 
1. Ensure that Golang is installed 
2. Run go mod init main and go mod tidy
3. Ensure that Docker and Pgadmin are insalled 
4. Run go get github.com/kelvins/geocoder
5. Run docker-compose up --build
6. To check on Databases go to Pgadmin and click on add server, then add server with hostname as localhost and Port as 5432 and password as root.

To restart and clean docker image run: 
docker stop $(docker ps -aq)      
docker rm $(docker ps -aq)
docker rmi $(docker images -aq)
docker images prune -a
docker compose down --volumes
