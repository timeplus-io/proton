DROP STREAM IF EXISTS t_nested_tuple;

create stream t_nested_tuple
(
    endUserIDs tuple(
      _experience tuple(
          aaid tuple(
              id Nullable(string),
              namespace tuple(
                  code low_cardinality(Nullable(string))
              ),
              primary low_cardinality(Nullable(uint8))
          ),
          mcid tuple(
              id Nullable(string),
              namespace tuple(
                  code low_cardinality(Nullable(string))
              ),
              primary low_cardinality(Nullable(uint8))
          )
      )
  )
)
ENGINE = MergeTree ORDER BY tuple();

SET output_format_json_named_tuples_as_objects = 1;

INSERT INTO t_nested_tuple FORMAT JSONEachRow {"endUserIDs":{"_experience":{"aaid":{"id":"id_1","namespace":{"code":"code_1"},"primary":1},"mcid":{"id":"id_2","namespace":{"code":"code_2"},"primary":2}}}};

SELECT * FROM t_nested_tuple FORMAT JSONEachRow;
SELECT endUserIDs._experience FROM t_nested_tuple FORMAT JSONEachRow;
SELECT endUserIDs._experience.aaid FROM t_nested_tuple FORMAT JSONEachRow;
SELECT endUserIDs._experience.aaid.id FROM t_nested_tuple FORMAT JSONEachRow;
SELECT endUserIDs._experience.aaid.namespace FROM t_nested_tuple FORMAT JSONEachRow;
SELECT endUserIDs._experience.aaid.namespace.code FROM t_nested_tuple FORMAT JSONEachRow;
SELECT endUserIDs._experience.aaid.primary FROM t_nested_tuple FORMAT JSONEachRow;

DROP STREAM t_nested_tuple;
