CREATE TABLE IF NOT EXISTS iris (
  sepal_length Float64,
  sepal_width Float64,
  petal_length Float64,
  petal_width Float64,
  species String)
ENGINE = MergeTree
PARTITION BY species
ORDER BY (species)
