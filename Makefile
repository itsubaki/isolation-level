SHELL := /bin/bash

runmysql:
	-docker pull mysql:5.7
	-docker stop mysql
	-docker rm mysql
	docker run --name mysql -e MYSQL_ROOT_PASSWORD=secret -p 3306:3306 -d mysql:5.7
	docker ps
	# mysql -h127.0.0.1 -P3306 -uroot -psecret -Dsecret
