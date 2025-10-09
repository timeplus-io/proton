proton-nlog is a tool to benchmark native_log performance.

before produce, it will create empty folder  `"./proton-nlog-perf/nativelog/log"` in current path.


## Example Usage
### Produce Logs: 

```
proton nlog produce --num_records 100000 --batch_size 1000 --event_size 1000
```

### Consume Logs:

```
proton nlog consume
```

## sample output

```
$ ../proton nlog produce --num_records 100000 --batch_size 1000 --event_size 1024
....
....
2024.03.26 01:11:15.495968 [ 880961 ] {} <Information> native_log: 

Produce Metrics:
Records: 100
Events: 100000
Total Bytes: 119600000
Record Batch Size: 1000
Event Size: 1196
Record Size: 1196000
Records per Second (RPS): 191.09
Events per Second (EPS): 191092.70
Bytes per Second (BPS): 228546874.36
Gigabytes per Second (GBps): 0.21
Elapsed Time: 0.523306216 seconds
```

```
$ ../proton nlog consume
....
2024.03.26 01:23:24.935094 [ 910248 ] {} <Information> native_log: 

Consumed Metrics:
Records Consumed: 100000
Total Bytes Consumed: 119600008
Events per Second (EPS): 341688.96
Bytes per Second (BPS): 408660027.50
Gigabytes per Second (GBps): 0.38
Elapsed Time: 0.29 seconds


```