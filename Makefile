SHELL := /bin/bash

test:
	go test -cover $(shell go list ./... | grep -v /vendor/ | grep -v /build/) -v -coverprofile=coverage.txt -covermode=atomic
	go tool cover -html=coverage.txt -o coverage.html

runmysql:
	-docker pull mysql:5.7
	-docker stop mysql
	-docker rm mysql
	docker run --name mysql -e MYSQL_ROOT_PASSWORD=secret -p 3306:3306 -d mysql:5.7
	docker ps
	# mysql -h127.0.0.1 -P3306 -uroot -psecret -Dsecret
