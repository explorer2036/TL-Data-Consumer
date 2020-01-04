Description:

	The Phase 1 Project is for the freelancer to learn and use an existing Data Consumer Go Application and to demonstrate their knowledge and capability of working with our application. 
	
	The Data Consumer is a server application within the following software architecture:
	
		Data Collector --> Kafka --> Data Consumer --> Postgres DB
	
	This starter phase allows us to see the freelancer's work so that we can entrust a far more complex project and software architecture/framework with the freelancer for future projects.

	This Phase 1 Project is divided into three parts:
	
		1) Create a Load Tester written in Go (to simulate many data collectors) to stress test the Data Consumer
		
		2) Map new data models to the Data Consumer
		
		3) Make suggestions for the existing design of the Data Consumer (in terms of efficiency and best practices)
	
Background:	
	
	The Data Consumer works with Kafka, Postgresql and Redis using the following Go libaries:
	
		"github.com/go-redis/redis"
		"github.com/lib/pq"
		"github.com/patrickmn/go-cache"
		"github.com/segmentio/kafka-go"
		log "github.com/sirupsen/logrus"	
	
	In total, there are about 400 lines of very tightly written Golang code in the application.
	
	The Data Consumer is designed to take messages off the Kafka broker (with SSL support) and parse the message contents. Using the message contents, a data row is stored into a Postgresql data table mapped from a Go struct.
	
	The Data Consumer will also get basic user token information from a Redis store.

	The Data Consumer uses concurrent routines to get data from Kafka and store data in the Postgres DB (as DB inserts). 
	
		State (insert):
			user | source | path | type | value | time | timestamp

		Metric (insert):
			user | source | path | value | time | timestamp
			
	Their Posgres DB tables are the following:
	
		DROP TABLE IF EXISTS state;
			CREATE TABLE state (
			  userid VARCHAR(100) NOT NULL,
			  source VARCHAR(100) NOT NULL,
			  path VARCHAR(200) NOT NULL,
			  type VARCHAR(20) NOT NULL,
			  value VARCHAR(10) NOT NULL,
			  time TIMESTAMPTZ  NOT NULL,
			  timestamp TIMESTAMPTZ  NOT NULL
			);

		DROP TABLE IF EXISTS metric;
			CREATE TABLE metric (
			  userid VARCHAR(100) NOT NULL,
			  source VARCHAR(100) NOT NULL,
			  path VARCHAR(200) NOT NULL,  
			  value float NOT NULL,
			  time TIMESTAMPTZ  NOT NULL,
			  timestamp TIMESTAMPTZ  NOT NULL
			);

	The Go application structs look like this:
		
		// State defines the struct which maps to table 'state'
			type State struct {
				UserID    string
				Source    string
				Path      string
				Type      string
				Value     string
				Time      time.Time
				Timestamp time.Time
				Message   kafka.Message
			}
			
		// Metric defines the struct which maps to table 'metric'
			type Metric struct {
				UserID    string
				Source    string
				Path      string
				Value     float64
				Time      time.Time
				Timestamp time.Time
				Message   kafka.Message
			}
	
Project Details:

	Part 1: Load Tester for Data Consumer Application
		
		- Write a separate load tester in Go to put some generic data messages (according to specifications) into Kafka to be consumed by the existing data consumer. 

		- The load tester will need to do the following:
		
			- Create simulated/generic users in the Redis database

			- Put JSON messages into Kafka at different sending rates defined by user parameters
			
			- Create some random values for metrics and states data so we can test the Data Consumer loads
			
			- Measure the rate at which the Data Consumer can consume and insert the messages into the postgres database, and measure the load of the Go Application and the Linux server it runs on during the stress test.
			
			- Export/Output this load information so it could be used by other monitoring systems
			
	Part 2: Add New Data Models to the Data Consumer Application
	
		The freelancer will add and test the following new data types:
		
			Log (insert):
				user | source | path | type | category | level | message | time | timestamp
				
			Network Trace (insert):
				user | source | path | type | trace | hop | host | latency | time | timestamp
				
			Network Ping (insert):
				user | source | path | type | ping | host | latency | time | timestamp	
				
	Part 3: Document all work
		
Code Package:

	The code package for the freelancer to use will include:
	
		Data Consumer Go Application code
		Explanations of JSON Kafka message format

Extra Requirements:

	You will need to setup a test environment. We can supply you with a VPS server to work on.
	
		- You can use dockers for simplicity of getting the go application programming/testing done, but the application should run on any setup, even non-docker, bare metal Ubuntu installations.
		
		The dockers we currently use are run like this:
		
			sudo docker run --name kafka -d -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 -p 9581-9585:9581-9585 -p 9092:9092 -p 9093:9093 -e ADV_HOST=209.159.148.254 -e ENABLE_SSL=1 -e SSL_EXTRA_HOSTS=xxx.xxx.xxx.xxx -v /home/usere/kafka/connectors:/connectors landoop/fast-data-dev
			
			sudo docker start kafka
			
			sudo docker run --rm -d --name timescale-docker -p 5432:5432 -v /home/usere/postgresql/data:/var/lib/postgresql/data -e POSTGRES_PASSWORD=XXXXXXX timescaledb

			sudo docker run --rm -d -p 6379:6379 -v /home/usere/redis/data:/data -v /home/usere/redis/redis.conf:/usr/local/etc/redis/redis.conf  --name traderlinked-redis redis redis-server /usr/local/etc/redis/redis.conf --appendonly yes
			
			We have a TLS guide for the Kafka docker that you can use to understand the SSL setup of Kafka.
		
		But you can propose other setups or methods for the test environment.		

	Document build requirements and procedures so the load tester application can be compiled for Ubuntu LTS 18.04
	
	Document build requirements and procedures so the updated Data Consumer application can be compiled for Ubuntu LTS 18.04


CREATE TABLE user_tbl(name VARCHAR(20), email VARCHAR(20));

CREATE UNIQUE INDEX CONCURRENTLY idx_user_name ON user_tbl (name);

psql -U postgres -d consumer -h 127.0.0.1 -p 5432

delete from user_tbl a where email<>(select min(email) from user_tbl b where a.name=b.name);
