CREATE TABLE IF NOT EXISTS iris (
  sepal_length float64,
  sepal_width float64,
  petal_length float64,
  petal_width float64,
  species string)
ENGINE = MergeTree
PARTITION BY species
ORDER BY (species)
