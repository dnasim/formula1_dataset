{{ config(materialized='view') }}  -- or 'table'

SELECT 
	year, 
	circuit_id, 
	circuit_name, 
	city, 
	country, 
	latitude as lat, 
	longitude as long, 
	wiki_url
FROM {{source("staging", "circuits_stg")}}
