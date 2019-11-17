
#!/bin/bash
set -e;
# FROM: https://github.com/docker-library/mongo/issues/329#issuecomment-460858099
# a default non-root role
MONGO_NON_ROOT_ROLE="${MONGO_NON_ROOT_ROLE:-readWrite}"

if [ -n "${MONGO_NON_ROOT_USERNAME:-}" ] && [ -n "${MONGO_NON_ROOT_PASSWORD:-}" ]; then
	"${mongo[@]}" "$MONGO_INITDB_DATABASE" <<-EOJS
		db.createUser({
			user: $(_js_escape "$MONGO_NON_ROOT_USERNAME"),
			pwd: $(_js_escape "$MONGO_NON_ROOT_PASSWORD"),
			roles: [ { role: $(_js_escape "$MONGO_NON_ROOT_ROLE"), db: $(_js_escape "$MONGO_INITDB_DATABASE") },
			 { role: "dbAdmin", db: $(_js_escape "$MONGO_INITDB_DATABASE")  },
			 { role: "dbOwner", db: $(_js_escape "$MONGO_INITDB_DATABASE") }
			]
			})
	EOJS
else
	# print warning or kill temporary mongo and exit non-zero
  exit 1
fi

#mongodump --host localhost --username "mhs" --password "mhs"  --authenticationDatabase mhs --gzip --archive=/Volumes/ComplexCareLab_Drive1/data/googledrive/ucdavis/data_links/mhs/mongo/mhs/dump/V01.gz

#mongorestore --username "root" --password "password" --gzip --archive="/data/dump/V01.gz"


#db.createUser({
#user: "mhs",
#pwd: "mhs",
#roles: [
#    { role: "read", db: "mhs" },
#    { role: "readWrite", db: "mhs" },
#    { role: "dbAdmin", db: "mhs" },
#    { role: "dbOwner", db: "mhs" },
#    {role: "userAdmin", db: "mhs" }
#    ]
#})

#docker run --rm --volumes-from my-mongo-server mongo unlink "/data/db/mongod.lock"
#docker run --rm --volumes-from my-mongo-server mongo --repair
#echo "RESTORING MONGO DATABASE"




#db.createUser({
#user: "sensor_organ_donation",
#pwd: "sensor_organ_donation",
#roles: [
#    { role: "read", db: "sensor_organ_donation" },
#    { role: "readWrite", db: "sensor_organ_donation" },
#    { role: "dbAdmin", db: "sensor_organ_donation" },
#    { role: "dbOwner", db: "sensor_organ_donation" },
#    {role: "userAdmin", db: "sensor_organ_donation" }
#    ]
#})