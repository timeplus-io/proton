CREATE AGGREGATE FUNCTION udf(col1 float32,col2 float32,col3 float32,col4 float32)
RETURNS string
LANGUAGE JAVASCRIPT AS $${
  //A UDF to get recent 100 events and get their sum/count/min/max

  initialize: function () {
    this.last100 = [[], [], [], []];
    this.reset();
  },

  reset: function () {
    this.sum = 0;
    this.count = 0;
    this.min = Number.MAX_VALUE;
    this.max = Number.MIN_VALUE;
    this.result = {};
  },

  process: function (col1, col2, col3, col4) {
    let inputs = [col1, col2, col3, col4];
    for (let i = inputs[0].length - 1; i >= 0; i--) {
      //add data to the beginning of the arrays
      for (let j = 0; j < 4; j++) {
        this.last100[j].unshift(inputs[j][i]);
      }
    }
    this.reset();
    for (let i = 0; i < 4; i++) {
      //get the recent 100 values
      this.last100[i].length = Math.min(this.last100[i].length, 100);
      //calc sum/count/min/max
      for (let j = this.last100[i].length - 1; j >= 0; j--) {
        this.sum += this.last100[i][j];
        this.count++;
        if (this.last100[i][j] > this.max) {
          this.max = this.last100[i][j];
        }
        if (this.last100[i][j] < this.min) {
          this.min = this.last100[i][j];
        }
      }
    }
    //return result as a json
    this.result = {
      sum: this.sum,
      count: this.count,
      min: this.min,
      max: this.max,
    };
  },

  finalize: function () {
    let old_results = this.result;
    this.result = {};
    return JSON.stringify(old_results);
  },
}
$$
;

CREATE RANDOM STREAM test500k(
  a float32 default rand(),
  b float32 default rand()%1000/10,
  c float32 default rand()%1000/100,
  d float32 default rand()%10000/5
  )
  SETTINGS eps=500000
;

select udf(a,b,c,d) from test500k EMIT PERIODIC 1s;
