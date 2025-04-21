## Lab 1: Robust DBMS for transactional processing

François Thievenon
François Gonon

#Setup : 

- Step 1 : docker containers setup

`docker pull mariadb`

`docker run --name myMariaDB -p 3306:3306 -e MARIADB_ROOT_PASSWORD='#TOREPLACE' mariadb`

`docker pull phpmyadmin`

`docker run --name myAdmin -d -e PMA_ARBITRARY=1 -p 8080:8080 phpmyadmin`