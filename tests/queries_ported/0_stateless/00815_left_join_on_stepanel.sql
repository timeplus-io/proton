-- Tags: no-parallel

CREATE DATABASE IF NOT EXISTS test_00815;
USE test_00815;

DROP STREAM IF EXISTS fact_cpc_clicks;
DROP STREAM IF EXISTS dim_model;

CREATE STREAM fact_cpc_clicks (model_id uint8) ENGINE = Memory;
CREATE STREAM dim_model (model_id uint8) ENGINE = Memory;

INSERT INTO fact_cpc_clicks VALUES (1);
INSERT INTO dim_model VALUES (1);

select f.model_id from fact_cpc_clicks as f left join dim_model as d on f.model_id=d.model_id limit 10;

USE default;

select f.model_id from test_00815.fact_cpc_clicks as f left join test_00815.dim_model as d on f.model_id=d.model_id limit 10;

DROP DATABASE test_00815;
