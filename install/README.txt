go test -v -args -f 2000 -c 21

#### login ui
curl -v -H "Content-type: application/json" -X POST -d '{"loginid":"alon@traderlinked.com","password":"XjsAZxOajA"}' http://localhost:5001/login

#### source_path

CREATE SEQUENCE source_path_id_sequence
  start 1000
  increment 1;

CREATE TABLE tl_source_path (
  id SERIAL PRIMARY KEY,
	path VARCHAR(100) NOT NULL,
	source VARCHAR(100) NOT NULL
);

#### errors

fixed_columns: ["userid", "type"]
relations:
  - dtype: "data_errors"
    table: "errors"
    columns: [""]

CREATE TABLE errors (
	userid VARCHAR(100) NOT NULL,
	type VARCHAR(20) NOT NULL
);

#### heartbeat

// The yaml format in consul
fixed_columns: ["userid", "source_path_id"]
relations:
  - dtype: "data_heartbeat"
    table: "heartbeat"
    columns: ["timestamp", "status"]

// The json format
{
    "dtype": "data_heartbeat",
    "action": "insert",
    "userid": "xxxx",
    "source": "xxxx",
    "path": "xxxx",
    "timestamp": "xxxx",
    "data": {
        "status": "OK"
    }
}

CREATE TABLE heartbeat (
	userid integer,
	source_path_id integer,
	status VARCHAR(10) NOT NULL,
	timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE metric (
	userid integer,
	source_path_id integer,
	value FLOAT(2) NOT NULL,
	time TIMESTAMPTZ NOT NULL,
	timestamp TIMESTAMPTZ NOT NULL
);

fixed_columns: ["userid", "source_path_id", "time", "timestamp"]
relations:
  - dtype: "data_metric"
    table: "metric"
    columns: ["value"]
  - dtype: "data_state"
    table: "state"
    columns: ["type", "value"]

CREATE TABLE state (
	userid integer,
	source_path_id integer,
	type VARCHAR(50) NOT NULL,
	value VARCHAR(50) NOT NULL,
	time TIMESTAMPTZ NOT NULL,
	timestamp TIMESTAMPTZ NOT NULL,
)
