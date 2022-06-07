# Results of scan server testing

Testing was performed on the [new Accumulo scan server feature](https://github.com/apache/accumulo/pull/2665) to see how it behaved in a few different scenarios.  The test all ran against a small amount of ContinuousIngest data in a table with 20 tablets.  Below is what the test data looked like.  The test ran this new class [ContinuousQuery](../src/main/java/org/apache/accumulo/testing/continuous/ContinuousQuery.java).

```
root@accumulo-testing> scan -t accumulo.metadata -c file
1;0666666666666667 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-0000046/A00000cr.rf [] 5317109,149925
1;0cccccccccccccce file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-0000047/A000007w.rf [] 5309606,149756
1;1333333333333335 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-0000048/A00000ct.rf [] 5323107,150232
1;199999999999999c file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-0000049/A000007y.rf [] 5336848,150224
1;2000000000000003 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004a/A0000005.rf [] 5330425,149955
1;266666666666666a file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004b/A00000cu.rf [] 5338047,150126
1;2cccccccccccccd1 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004c/A00000cv.rf [] 5346600,150409
1;3333333333333338 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004d/A0000006.rf [] 5326093,149738
1;399999999999999f file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004e/A000007u.rf [] 5352169,150675
1;4000000000000006 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004f/A0000007.rf [] 5306946,149412
1;466666666666666d file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004g/A00000cw.rf [] 5309084,149312
1;4cccccccccccccd4 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004h/A00000cs.rf [] 5320576,149987
1;533333333333333b file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004i/A000007z.rf [] 5334960,150186
1;59999999999999a2 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004j/A00000cq.rf [] 5317236,149628
1;6000000000000009 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004k/A0000003.rf [] 5331973,150353
1;666666666666667 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004l/A0000002.rf []  5323213,149751
1;6cccccccccccccd7 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004m/A0000004.rf [] 5325651,149963
1;733333333333333e file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004n/A000007v.rf [] 5323228,150219
1;79999999999999a5 file:hdfs://10.0.2.6:8000/accumulo/tables/1/t-000004o/A000007x.rf [] 5331272,150392
1< file:hdfs://10.0.2.6:8000/accumulo/tables/1/default_tablet/A0000001.rf []    5325093,149756
```

The following variables were adjusted prior to starting each test.

 * **Busy timeout** : Scan servers offer a feature that tablet servers do not have called busy timeout.  An Accumulo client can specify a busy timeout when requesting a scan on a scan server. If the scan does not start running within the requested busy timeout, then the scan server will return to the client which can choose another scan server.  
 * **Initial servers** : Accumulo clients choose which scan server to send a request via a client side plugin.  The default plugin hashes tablets to scans servers so that different client instances will choose the same scan servers for a given tablet.  Sending scans for the same tablet to the same scan server helps increase cache utilization on scan servers.  The tests were configured to choose a random scan server from an initial small set of scan servers.  If the initial scan got a busy timeout, then the tests were configured to randomly choose scan server from all available scan servers.
 * **# scan servers** : This is the total number of scans servers to run for the test.
 * **scan type** : When set to *immediate* tablet servers were used to execute scans.  When set to *eventual* scan servers were used to execute scans.
 * **#concurrent** : The total number or concurrent threads that would be executing scans during a test.  These threads were running in multiple VMs.
 * **workload config** : The types of scans run for a test were adjusted via config.  Three different test types were run which are discussed later.

All test were run by starting a kubernetes deployment (an example is [here](query-job.yaml)) that would create a lot of concurrent activity against scan servers or tservers w/ very little logging.  After starting that, another kubernetes deployment (an example is [here](query-single.yaml)) with a single pod with a single thread and trace level logging would be started.  This single thread would execute a configured number of scans and then exit.  The second deployment was configured such that it would always finish before the first deployment.  The activity of the second single threaded deployment was analyzed for this report. After running a test, the following results were collected from this analysis.

 * **# busy events** : The number of times a scan server kicked back a busy event to the Accumulo client.  
 * **avg time** : The average time that scans took.
 * **std dev time** : The standard deviation from the average.
 * **min time** : The minimum observed time for a scan
 * **max time** : The maximum observed time for a scan

Before starting each test the test config(except for concurrency) for the two deployments were manually changed.  At the end of each test the deployment configs use for the test were copied and can be inspected in each test dir.  There are config maps in the deployment descriptors that contain accumulo testing config that was passed to the ContinuousQuery class used for testing.  The script [record-results.sh](record-results.sh) was used to collect results at the end of each test.

The following resources were used to run these test.

 * 1 X Standard_D8s_v4 VM running manager processes (Namenode, Zookeeper, Manager)
 * 3 X Standard_D8s_v4 VMs running tservers and datanodes.  Each VM had 8x64GB HDD.
 * 16 X Standard_D4ds_v5 VMs backing a Kubernetes node pool used to run scan test client.
 * 25 X Standard_D8ds_v5 VMs backing a Kubernetes node pool used to run scan servers.

Both the tserver and sserver used the default scan threads config, which is 16 threads.  The settings for thrift threads was upped to 512 to ensure that servers could process connections in a timely manner.  These were the settings `sserver.server.threads.minimum` and `tserver.server.threads.minimum`.  All of the config for the scan servers can be seen in [accumulo-scanservers.yaml](accumulo-scanservers.yaml), look at the accumulo.properties and accumulo-env.sh config maps.

## Large scans test

The first set of tests picked a random large range and scanned all of the data in it.  There were 193 threads spread across 16 VMs repeatedly executing these scans on random ranges.  All scans were within the same tablet. The following are statistics about the number of entries returned by these scans.

 * minResults:32.0 
 * maxResults:134723.0 
 * avgResults:36712.650000000016 
 * stddevResults:33438.24773598358

So on average each of the 193 threads were repeatedly executing scans that returned 36,712 entries.  The following are the different tests that were run for this scenario.

Test | # Scan servers | #initial servers |   busy timeout | scan type | # concurrent | # busy events | avg time | std dev time | min time | max time
 --- | ---------------| ---------------- | -------------- | --------- | ------------ | ------------- | -------- | ------------ | -------- | --------
 [D806](tests/D806) | N/A | N/A | N/A | immediate |  193 | 0 | 9060ms | 8142ms | 230ms | 35638ms
 [D803](tests/D803) | 12 | 3 | 33ms | eventual |  193 | 93 | 1345ms | 1342ms | 32ms | 5249ms
 [D804](tests/D804) | 12 | 1 | 33ms | eventual |  193 | 273 | 456ms | 514ms | 32ms | 3740ms
 [D805](tests/D805) | 23 | 1 | 33ms | eventual |  193 | 283 | 338ms | 363ms | 38ms | 3043ms
 [D807](tests/D807) | 23 | 3 | 10ms | eventual |  193 | 256 | 291ms | 263ms | 16ms | 1398ms
 [D808](tests/D808) | 23 | 23 | 10ms | eventual | 193 | 0 | 228ms | 203ms | 2ms | 993ms

Test D806 shows that 193 concurrent scans against a single tablet on a single tablet server takes an average of 9 seconds per scan.  Test D803 uses 12 scan servers and considerably reduces the average scan time from 9 seconds to 1.3 seconds.  However test D803 does not have many busy events, which would cause other scan servers than the initial 3 to be used.  Test D804 lowered the number of initial scan servers, putting more load on that single initial scans server which caused more busy events.   The increase in busy events caused other scan servers to be used, lowering the average scan time to 456ms.  Test D805 increased the number of scan servers, so that when a busy event did occur there was a larger pool of scan servers to choose from, which further decreased the average scan time.  Compare test D807 to D805 and D803, instead of lowering the initial servers it lowered the busy timeout which caused load to shed to other scan servers and resulted in a really good average scan time.  Test D808 set the initial scan servers to 23, yielding the best possible average scan time for this situation.  Test D808 is good for comparison, it shows that test D807 can approach the best possible time using busy timeout without always going to all severs which is terrible for cache utilization.

The basic pattern that seems to emerge here is that performance on the initial set of servers will degrade until they reach a threshold were more load is shed because of busy events at which point performance will start to improve.  However the ability to shed load allows even a single initial scan server to eventually get much better performance than a single tablet server which can not shed load.

## Grep scan test

This set of tests would read a similar amount of data server side as the previous test, but would grep for `def` in the row using an accumulo iterator.  The rows were random hex data so this would occur sometimes in the row.  So these test would execute lots of computation and return little data.  The following are statistics about the number of entries returned by these scans.

 * minResults:0.0 
 * maxResults:1074.0 
 * avgResults:269.5190000000001 
 * stddevResults:242.8214485890709

The following are the different test that were run for this scenario. All concurrent scans were run against a single tablet.

Test | # Scan servers | #initial servers |   busy timeout | scan type | # concurrent | # busy events | avg time | std dev time | min time | max time
 --- | ---------------| ---------------- | -------------- | --------- | ------------ | ------------- | -------- | ------------ | -------- | --------
[D816](tests/D816) | N/A | N/A | N/A | immediate | 193 | 0 | 3203ms | 427ms | 2391ms | 72960ms
[D815](tests/D815) | 12 | 3 | 33ms | eventual |  193 | 1022 | 218ms | 168ms | 32ms | 983ms
[D811](tests/D811) | 23 | 3 | 33ms | eventual |  193 | 954 | 160ms | 124ms | 36ms | 886ms
[D812](tests/D812) | 23 | 3 | 10ms | eventual |  193 | 924 | 146ms | 133ms | 12ms | 926ms
[D813](tests/D813) | 23 | 23 | 33ms | eventual |  193 | 21 | 138ms | 133ms | 2ms | 729ms
[D814](tests/D814) | 23 | 12 | 33ms | eventual |  193 | 234 | 207ms | 185ms | 3ms | 1167ms

Test D813 represents the best possible time using 23 scan servers.  Test D811 and D812 compare very well to D813 while using a small set of initial servers.  All test are less than 1/10th of the time D816 which ran against the tablet server, this is the result of having so many more cores available.

The same grep test was run against 5 tablets instead of 1.  The way the random ranges are selected, expanding the tablet range meant less of the random ranges were truncated on the tablet boundaries.  This resulted in scanning around 2.5x more data being grepped than the previous test.  The following are statistics about the number of entries returned by these scans.

 * minResults:0.0
 * maxResults:2836.0 
 * avgResults:688.579 
 * stddevResults:593.1863550443577

The following are the different test that were run for this scenario.

Test | # Scan servers | #initial servers |   busy timeout | scan type | # concurrent | # busy events | avg time | std dev time | min time | max time
 --- | ---------------| ---------------- | -------------- | --------- | ------------ | ------------- | -------- | ------------ | -------- | --------
[D821](tests/D821) |  N/A | N/A | N/A | immediate | 193 | 0 | 4670ms | 4295ms | 5ms | 21248ms
[D817](tests/D817) | 12 | 3 | 33ms | eventual | 193 | 1656 | 589ms | 470ms | 14ms | 2762ms
[D818](tests/D818) | 23 | 3 | 33ms | eventual | 193 | 763 | 439ms | 386ms | 4ms | 2429ms
[D819](tests/D819) | 23 | 1 | 33ms | eventual | 193 | 1509 | 391ms | 317ms | 20ms | 1669ms 
[D820](tests/D820) | 23 | 23 | 33ms | eventual | 193 | 48 | 348ms | 311ms | 5ms | 1484ms

For D821 since 5 tablets were scanned, all 3 tablet servers were used.  The reason the avg time for D821 is worse than D816 which only used a single tablet server is because the increase in data being scanned.  Would like to circle back and modify the scan test code to create ranges that scan consistent amounts of data for these two different scenarios.  This would make comparing multiple to single tablet tests possible.  Would be really nice to able to compare the times in the single and multi-tablet test.  

With multiple tablets, each tablet will have a different set of initial scan servers which spreads the initial load more than when everything goes against a single tablet.  Test D819 has initial severs configured to 1, so each of the 5 tablets will hash to one of the 23 servers. If all 5 tablets hash to different servers, then the load on those 5 needs to reach a threshold where busy events occur. For this level of concurrency the 5 initial severs in test D819 reach that threshold sooner than the 5x3 initial severs in D818.  D819 compares well to D820 which is the best possible time.

## Small scan test

This set of tests ran a lot of concurrent small scans against a single tablet.  The data returned by scans had the following characteristics.

 * minResults:0.0 
 * maxResults:715.0 
 * avgResults:184.1709999999997 
 * stddevResults:158.8509917431709

Test | # Scan servers | #initial servers |   busy timeout | scan type | # concurrent | # busy events | avg time | std dev time | min time | max time
 --- | ---------------| ---------------- | -------------- | --------- | ------------ | ------------- | -------- | ------------ | -------- | --------
[D824](tests/D824) |  N/A | N/A | N/A | immediate | 721 | 0 | 548ms | 37ms | 370ms | 880ms
[D833](tests/D833) | 3 | 1 | 33ms | eventual | 721 | 2645 | 92ms | 39ms | 37ms | 790ms
[D832](tests/D832) | 6 | 1 | 33ms | eventual | 721 | 2052 | 60ms | 23ms | 38ms | 648ms
[D825](tests/D825) | 12 | 3 | 33ms | eventual | 721 | 1621 | 53ms | 22ms | 28ms | 687ms
[D826](tests/D826) | 12 | 3 | 5ms | eventual | 721 | 5919 | 53ms | 30ms | 14ms | 1008ms
[D830](tests/D830) | 23 | 23 | 33ms | eventual | 721 | 132 | 51ms |19ms | 1ms | 598ms

In these tests as the number of scan servers was increased from 12 to 23, there was not a decrease in average time. Investigation showed that maybe this was because datanodes were tapped out.  There were only 3 datanodes and they were all observed to have more than 400% cpu while the test was running against scan servers.  Also noticed in test D824 against the tablet sever that only the datanode colocated with the tserver was running at high load and the tserver was not using all 100% on all cores, maybe the tserver would have done better if it used all three datanode like the scan servers did.  Also noticed the scan server CPU load were not that high, like they were in previous test that burned more scan server and tablet server CPU.

Since these scans were so quick, an attempt to use a small busy timeout was made in D826. However with the datanodes limiting performance, there was no detectable difference in this test other than seeing a lot more busy events.  This was probably caused by all servers looking busy with the datanode contention plus the short timeout.  Would have like to be able to configure the plugin to use a busy timeout of 5ms on the 1st attempt and 33ms on the 2nd, but the current default pluging does not support that.

Since the test seemed constrained by datanodes when scaling up, tried scaling down in D833 and D832 in order to see a relative difference.  If the clusted had had more than 3 data nodes, would have tried increasing the replication of the data in DFS for testing purposes.

All test up to this point had the table setting `table.cache.block.enable` set to false, so only the rfile index data in the ci table was cached.   The setting was set to true so that rfile index and data were cached and the following test were run.

Test | # Scan servers | #initial servers |   busy timeout | scan type | # concurrent | # busy events | avg time | std dev time | min time | max time
 --- | ---------------| ---------------- | -------------- | --------- | ------------ | ------------- | -------- | ------------ | -------- | --------
 [D839](tests/D839) |  N/A | N/A | N/A | immediate | 1 | 0 | 2.5ms |  12ms | 0ms | 480ms
 [D838](tests/D838) |  1 | 1 | 33ms | eventual | 1 | 0 | 2.8ms | 15.6ms | 0ms | 615ms
 [D834](tests/D834) | N/A | N/A | N/A | immediate | 721 | 0 | 90ms | 15ms | 44ms | 513ms
 [D844](tests/D844) | N/A | N/A | N/A | immediate | 1441 | 0 | 179ms | 19ms | 116ms | 470ms
 [D845](tests/D845) | N/A | N/A | N/A | immediate | 2881 | 0 | 529ms | 86ms | 348ms | 955ms
 [D835](tests/D835) | 12 | 1 | 5ms | eventual | 721 | 945 | 34ms | 20ms | 10ms | 702ms
 [D836](tests/D836) | 12 | 3 | 33ms | eventual | 721 | 1 | 18ms | 20ms | 1ms | 705ms
 [D840](tests/D840) | 12 | 3 | 5ms | eventual | 721 | 518 | 17ms | 19ms | 3ms | 765ms
 [D837](tests/D837) | 12 | 12 | 33ms | eventual | 721 | 0 | 5.0ms | 21ms | 0ms | 943ms
 [D841](tests/D841) | 23 | 3 | 5ms | eventual | 721 | 571 | 17ms | 18ms | 2ms | 724ms
 [D843](tests/D843) | 23 | 3 | 5ms | eventual | 1441 | 705 | 27ms | 26ms | 3ms | 890ms
 [D846](tests/D846) | 23 | 3 | 5ms | eventual | 2881 | 763 | 43ms | 54ms | 4ms | 2002ms
 [D842](tests/D842) | 23 | 23 | 5ms | eventual | 721 | 0 | 4.6ms | 24ms | 0ms | 1077ms
 [D847](tests/D847) | 23 | 23 | 5ms | eventual | 2881 | 0 | 4.1ms | 20ms | 0ms | 867ms
 
Tests D839 and D838 only had a single thread total running, for this case the scan server and tablet server are very similar.  Things were much faster (for example compare D836 to D825 OR D842 to D830 these are the same test except for cache) with the cache and the datanode bottlenecks were avoided, which allowed experimenting with much higher levels of concurrency.  Looking at tests D834,D844, and D845 as the amount of concurrecy doubles the average times increase by double or more.  In tests D841,D843, and D846 as the concurrency doubles, the increase in average times is less than double which is nice.  Tests D837,D842, and D847 show that when all scan servers are used initially with high concurrency that the times are close to the single threaded tests D839 and D838.  In test D836 there was only one busy event, so that means most of the scans were processed by the three initial severs.  Tests D840 lower the busy timeout has a lot more busy events and gets slight better performance than D836.  Test D835 had one initial server and the most busy events.  Since D835 initially sends all scans to a single scan server, its similar to the tserver test D834 except the tserver test does not shed load when busy.

# Conclusion

These tests show that scan severs are working really well and can support scaling out using the busy timeout primitive.  Based on this testing, thinking it would be good to simplify the default scan server dispatcher plugin to make it purely configuration driven.  The default plugin has an algorithm, it would be more useful to replace this with explicit config that says how many scan severs to use on the 1st busy timeout, how many to use on the 2nd, etc.  This would make the default plugin extremely flexible and easier to understand.  This can be follow on work to the initial scan server PR.

While running these test, the scan server were running in a Kubernetes deployment.  This deployment was scaled up and down, stopped, and started.  The tservers and manager were restarted while the scan servers were running.  All of this activity caused zero problems, which is a test in itself.

More testing needs to be done.  Need to cover using the batch scanner and run more test with multiple tablets.  Also this testing only looked at performance, but did not verify the data returned by scan servers and tablet servers were correct.


