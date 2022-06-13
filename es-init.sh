#!/bin/sh

if [ x${ELASTIC_PASSWORD} == x ]; then
	echo "Set the ELASTIC_PASSWORD environment variable in the .env file";
        exit 1;
fi;

if [ ! -f config/certs/ca.zip ]; then
	echo "Creating CA";
        bin/elasticsearch-certutil ca --silent --pem -out config/certs/ca.zip;
        unzip config/certs/ca.zip -d config/certs;
fi;

if [ ! -f config/certs/certs.zip ]; then
	echo "Creating certs";
        echo -ne \
		"instances:\n"\
		"  - name: es\n"\
		"    dns:\n"\
		"      - es\n"\
		"      - localhost\n"\
		"    ip:\n"\
		"      - 127.0.0.1\n"\
		> config/certs/instances.yml;
		bin/elasticsearch-certutil cert --silent --pem\
			-out config/certs/certs.zip\
			--in config/certs/instances.yml\
			--ca-cert config/certs/ca/ca.crt\
			--ca-key config/certs/ca/ca.key;
		unzip config/certs/certs.zip -d config/certs;
fi;

echo "Setting file permissions"

chown -R root:root config/certs;

find . -type d -exec chmod 750 \{\} \;;
find . -type f -exec chmod 640 \{\} \;;

echo "Waiting for Elasticsearch availability";

until curl -s --cacert config/certs/ca/ca.crt https://es:9200 | grep -q "missing authentication credentials"; do sleep 30; done;

echo "All done!";

exit(0)

