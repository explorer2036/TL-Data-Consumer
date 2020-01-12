go test -v -args -f 2000 -c 21

CREATE TABLE metric (
	userid VARCHAR(100) NOT NULL,
	source VARCHAR(100) NOT NULL,
	path VARCHAR(200) NOT NULL,
	value FLOAT(2) NOT NULL,
	time TIMESTAMPTZ NOT NULL,
	timestamp TIMESTAMPTZ NOT NULL,
)

CREATE TABLE state (
	userid VARCHAR(100) NOT NULL,
	source VARCHAR(100) NOT NULL,
	path VARCHAR(200) NOT NULL,
	type VARCHAR(50) NOT NULL,
	value VARCHAR(50) NOT NULL,
	time TIMESTAMPTZ NOT NULL,
	timestamp TIMESTAMPTZ NOT NULL,
)

CREATE TABLE heartbeat (
	userid VARCHAR(100) NOT NULL,
	source VARCHAR(100) NOT NULL,
	path VARCHAR(200) NOT NULL,
	status VARCHAR(4) NOT NULL,
	timestamp TIMESTAMPTZ NOT NULL,
)

fixed_columns: ["userid", "source", "path", "time", "timestamp"]
relations:
  - dtype: "data_metric"
    table: "metric"
    columns: ["value"]
  - dtype: "data_state"
    table: "state"
    columns: ["type", "value"]