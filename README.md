# Overview 

This branch presents the transaction management technique in [A Transactional Perspective on Execute-order-validate Blockchains](https://arxiv.org/abs/2003.10064), published in SIGMOD 2020. Refer to [sharpscheduler](orderer/common/blockcutter/scheduler/sharpscheduler/scheduler.go) for details.
In addition, we also provide the implementation of two OCC-related baselines in the paper:
* focc-standard (detailed in [standardscheduler](orderer/common/blockcutter/scheduler/standardscheduler/scheduler.go) )
* focc-latest (detailed in [latestscheduler](orderer/common/blockcutter/scheduler/latestscheduler/scheduler.go))

# To Build
Build `peer`, `orderer` executables as well as their docker images as normal. 
```
make peer
make orderer
```
or
```
make peer-docker
make orderer-docker
```

# To Run
## Sharp scheduler (Our proposal)
```
SCHEDULER_TYPE=sharp STORE_PATH=/tmp/mvs TXN_SPAN_LIMIT=10 [...other configurations] orderer > orderer.log
```
```
SCHEDULER_TYPE=sharp [...other configurations] peer node start > peer.log
```
* `STORE_PATH` is the directory path of the LevelDB instance that implements the multi-versioned storage, used to compute the transaction dependency. The directory will be emptied **every time** that the binary runs. 
* `TXN_SPAN_LIMIT` restricts the maximum block span of a transaction, as described in the paper. 

## Focc-standard 
```
SCHEDULER_TYPE=standard STORE_PATH=/tmp/mvs [...other configurations] orderer > orderer.log
```
```
SCHEDULER_TYPE=standard [...other configurations] peer node start > peer.log
```
* `STORE_PATH` is similar as above.

## Focc-latest
```
 SCHEDULER_TYPE=latest [...other configurations] orderer > orderer.log
```
```
SCHEDULER_TYPE=latest [...other configurations] peer node start > peer.log
```

# Note
* Sharp scheduler is used by default, if `SCHEDULER_TYPE` is unset or empty, 
* For the ease of the configuration, users can optionally provide the block size by setting the global environment `BLOCK_SIZE`. By default, it uses the configuration in _configtx.yaml_. 
* In the paper, there are two other baselines, _Fabric++_ and _original_. The former is in this [repo](https://github.com/sh-ankur/fabric). The latter can be downloaded from the official Fabric [repo](https://github.com/hyperledger/fabric) and checkout the version with commit `afbc66a1b8c9ecafca8c64159739373f57b72e46`.
* We provide the log analysis [scripts](supplementary/scripts), which measures the internal fine-grained timing from the log
  * `LOG_LEVEL` must be set to `INFO` during execution
  * Sample usage, assumed wth Focc-latest scheduler. 
  ```
  python supplementary/scripts/process_occ-latest_orderer.py orderer.log
  ```
  ```
  python supplementary/scripts/process_occ-latest_peer.py peer.log
  ```
* We also provide the benchmark contract(chaincode) [custom.go](supplementary/contract/custom.go). In the contract, 
  * we invoke `ReadModifyWrite()` and `function=empty` to get **Figure 1** in **Sec 1 Introduction** in the paper. 
  * We invoke `ReadWrite()` for the remaining experimental charts. 
* [Old Readme](old_README.md)
