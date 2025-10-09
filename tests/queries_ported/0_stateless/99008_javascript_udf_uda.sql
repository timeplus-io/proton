-- Tags: no-parallel

CREATE OR REPLACE FUNCTION js_func_1(value float32) RETURNS float32 LANGUAGE JAVASCRIPT AS $$
function js_func_1(value) {
    for(let i=0;i<value.length;i++) {
        value[i]=value[i]+1;
    }
    return value;
}
$$;

CREATE FUNCTION js_func_1(value float32) RETURNS float32 LANGUAGE JAVASCRIPT AS $$
function js_func_1(value) {
    for(let i=0;i<value.length;i++) {
        value[i]=value[i]+1;
    }
    return value;
}
$$; --{serverError 609}

CREATE FUNCTION IF NOT EXISTS js_func_1(value float32) RETURNS float32 LANGUAGE JAVASCRIPT AS $$
function js_func_1(value) {
    for(let i=0;i<value.length;i++) {
        value[i]=value[i]+1;
    }
    return value;
}
$$;

CREATE OR REPLACE FUNCTION js_func_1(value float32) RETURNS float32 LANGUAGE JAVASCRIPT AS $$
function js_func_1(value) {
    for(let i=0;i<value.length;i++) {
        value[i]=value[i]+5;
    }
    return value;
}
$$;

CREATE OR REPLACE AGGREGATE FUNCTION js_func_2(value float32, _tp_delta int8) RETURNS float32 LANGUAGE JAVASCRIPT AS $$
{
    initialize: function() {
        this.sec = -1.0
    },
    process: function(values) {
        for (let i = 0; i < values.length; i++) {
            if (values[i] > this.sec)
                this.sec = values[i];
        }
    },
    finalize: function() {
        return this.sec;
    },
    serialize: function() {
        let s = { 'sec': this.sec };
        return JSON.stringify(s)
    },
    deserialize: function(state_str) {
        let s = JSON.parse(state_str);
        this.sec = s['sec'];
    },
    merge: function(state_str) {
        let s = JSON.parse(state_str);
        if (s['sec'] >= this.sec) { this.sec = s['sec'] }
    }
}
$$;

CREATE AGGREGATE FUNCTION js_func_2(value float32, _tp_delta int8) RETURNS float32 LANGUAGE JAVASCRIPT AS $$
{
    initialize: function() {
        this.sec = -1.0
    },
    process: function(values) {
        for (let i = 0; i < values.length; i++) {
            if (values[i] > this.sec)
                this.sec = values[i];
        }
    },
    finalize: function() {
        return this.sec;
    },
    serialize: function() {
        let s = { 'sec': this.sec };
        return JSON.stringify(s)
    },
    deserialize: function(state_str) {
        let s = JSON.parse(state_str);
        this.sec = s['sec'];
    },
    merge: function(state_str) {
        let s = JSON.parse(state_str);
        if (s['sec'] >= this.sec) { this.sec = s['sec'] }
    }
}
$$; --{serverError 609}

CREATE AGGREGATE FUNCTION IF NOT EXISTS js_func_2(value float32, _tp_delta int8) RETURNS float32 LANGUAGE JAVASCRIPT AS $$
{
    initialize: function() {
        this.sec = -1.0
    },
    process: function(values) {
        for (let i = 0; i < values.length; i++) {
            if (values[i] > this.sec)
                this.sec = values[i];
        }
    },
    finalize: function() {
        return this.sec;
    },
    serialize: function() {
        let s = { 'sec': this.sec };
        return JSON.stringify(s)
    },
    deserialize: function(state_str) {
        let s = JSON.parse(state_str);
        this.sec = s['sec'];
    },
    merge: function(state_str) {
        let s = JSON.parse(state_str);
        if (s['sec'] >= this.sec) { this.sec = s['sec'] }
    }
}
$$;

CREATE OR REPLACE AGGREGATE FUNCTION js_func_2(value float32, _tp_delta int8) RETURNS float32 LANGUAGE JAVASCRIPT AS $$
{
    initialize: function() {
        this.max = -1.0;
        this.sec = -1.0
    },
    process: function(values) {
        for (let i = 0; i < values.length; i++) {
            if (values[i] > this.max) {
                this.sec = this.max;
                this.max = values[i];
            }
            if (values[i] < this.max && values[i] > this.sec)
                this.sec = values[i];
        }
    },
    finalize: function() {
        return this.sec;
    },
    serialize: function() {
        let s = { 'max': this.max, 'sec': this.sec };
        return JSON.stringify(s)
    },
    deserialize: function(state_str) {
        let s = JSON.parse(state_str);
        this.max = s['max'];
        this.sec = s['sec'];
    },
    merge: function(state_str) {
        let s = JSON.parse(state_str);
        if (s['sec'] >= this.max) { this.max = s['max']; this.sec = s['sec'] }
        else if (s['max'] >= this.max) { this.sec = this.max; this.max = s['max'] }
        else if (s['max'] > this.sec) { this.sec = s['max'] }
    }
}
$$;

show create function js_func_1;
show create function js_func_2;
show functions where language = 'Javascript' and ( name = 'js_func_1' or name = 'js_func_2');

drop function js_func_1;
drop function js_func_2;

--- Unsupported cases:
drop stream if exists 99008_stream;
drop function if exists stock_99008;
CREATE OR REPLACE AGGREGATE FUNCTION stock_99008(rowtime datetime64(6), price float32) returns tuple(start_price float32, bottom_ts datetime64(1), end_price float32, down_duration uint64) LANGUAGE JAVASCRIPT AS
$$
{
  has_customized_emit:true,
  initialize:function(){this.bottom_ts=new Date();this.last_down_price=-1.0;this.start_price=-1.0;this.down_duration=1;this.result=[]},
  process:function(rowtime,price){for(let i=0;i<rowtime.length;i++){if(this.start_price<0||(this.last_down_price<0&&price[i]>=this.start_price)){this.start_price=price[i];this.bottom_ts=rowtime[i];this.down_duration=1}else if((this.last_down_price<0&&price[i]<this.start_price)||(price[i]<this.last_down_price)){this.last_down_price=price[i];this.bottom_ts=rowtime[i];this.down_duration=this.down_duration+1}else if(price[i]>this.last_down_price){this.result.push({'start_price':this.start_price,'bottom_ts':this.bottom_ts,'end_price':price[i],'down_duration':this.down_duration});this.bottom_ts=rowtime[i];this.start_price=price[i];this.last_down_price=-1.0;this.down_duration=1}else{this.down_duration=this.down_duration+1}}return this.result.length},
  finalize:function(){var old_result=this.result;this.result=[];return old_result},
  serialize:function(){let s={'bottom_ts':this.bottom_ts,'last_down_price':this.last_down_price,'start_price':this.start_price,'down_duration':this.down_duration,'result':this.result};return JSON.stringify(s)},
  deserialize:function(state_str){let s=JSON.parse(state_str);this.bottom_ts=s['bottom_ts'];this.last_down_price=s['last_down_price'];this.start_price=s['start_price'];this.down_duration=s['down_duration'];this.result=s['result'];},
  merge:function(){}
}
$$;

create stream 99008_stream(i int, i8 int8, f32 float32, s string, dt datetime64(6) default now64(6)) settings shards=3;
select s, st.1, st.2, st.3, st.4 from (select s, stock_99008(dt, f32) as st from 99008_stream group by s) order by s limit 0; -- { serverError UNSUPPORTED }
select s, st.1, st.2, st.3, st.4 from (select s, stock_99008(dt, f32) as st from 99008_stream shuffle by s group by s) order by s limit 0;

drop stream if exists 99008_stream;
drop function if exists stock_99008;
