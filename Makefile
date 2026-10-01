.PHONY: up down trigger logs password query tables clean

up:            ## build & start Spark Thrift + Airflow
	docker compose up -d --build

down:
	docker compose down

clean:         ## stop and delete ALL lakehouse data
	docker compose down -v

password:      ## Airflow admin password (user: admin)
	docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt; echo

trigger:       ## run the pipeline once
	docker compose exec airflow airflow dags trigger medallion_lakehouse

logs:
	docker compose logs -f airflow

query:         ## sample Gold query via beeline
	docker compose exec spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 \
	  -e "select * from gold.agg_daily_sales order by order_date desc limit 10"

tables:
	docker compose exec spark-thrift /opt/spark/bin/beeline -u jdbc:hive2://localhost:10000 \
	  -e "show tables in silver; show tables in gold"
