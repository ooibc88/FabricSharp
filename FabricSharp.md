f# Overview
In Fabric#, the _peer_ process relies on __Forkbase__[3] to replace the original LevelDB in order for the secure and efficient provenance storage. 
However, we have not released __ForkBase__ source code. 
Hence, _peer_ can not get freely built in any platforms. (E.g, running `make peer` will definitely fail. )
Instead, we provide a prebuilt __ForkBase__ dynamic library, along which _peer_ process can be built in the provided docker environment. 

# Quick Start
1. Build the chaincode environment  

   ```
   make ccenv
   ```
2. Build the forkbase image. Note, this process may take a while (around 10~20 minutes), as it needs downloading some dependency and copying large files. 

    ```
    make forkbase-docker 
    ``` 
3. Build the docker image 

 ```
 DOCKER_DYNAMIC_LINK=true make peer-docker 
 ``` 
<!-- 4. Test the build status by running

```
docker run hyperledger/fabric-peer peer version | grep 'Commit SHA'
```
It shall return `Commit SHA: 7f79c7f`. -->
4. Once finished, You can start the peer container with the default setup 
```
docker run hyperledger/fabric-peer peer node start
```
You are recommended to spin up the whole network with the docker-compose file in the official [fabric-sample repo](https://github.com/hyperledger/fabric-samples/blob/release-1.4/basic-network/docker-compose.yml).

5. (Optional) Meanwhile, we also provide a straightforward implementation that relies on the existing LevelDB to support provenance storage. It can be turned on by setting the environment variable 
```
docker run --env CORE_LEDGER_STATE_STATEDATABASE=goleveldb hyperledger/fabric-peer peer node start
```
Note, by default `CORE_LEDGER_STATE_STATEDATABASE=forkbase`

# Major Changes
## Development upon ForkBase
* We implement the Merkle DAG and Deterministic Append-only Skip List on top of Forkbase Storage, as described in [2]. Refer to [db.cc](images/forkbase/payload/src/db.cc) for details.
* We use tool _swig_ to install our implementation into a go package (, named as _ustore_).

## Fabric Codebase
* We additionally provide [stateustoredb.go](core/ledger/kvledger/txmgmt/statedb/stateustoredb/stateustoredb.go) that implements _versiondb_ interface, which will invoke the above-generated _ustore_ package. 
* We additionally provide three provenance APIs (_Hist_, _Backward_ and _Forward_) for _ChaincodeStubInterface_ and one overrided method _Prov_ for Chaincode in [shim.interfaces](core/chaincode/shim/interfaces.go). Refer to [2] for their usage detail. 
* Previously the chaincode will be launched and executed in a separate docker container. The data access request between the chaincode and peer process takes place in the format of grpc message. 
* Fabric# relies on the existing infrastructure for the data communication. But we spare special key words to denote for the provenance query and information. These keywords and the associated value will be treated differently before dumping into storage, as shown in function _GetState_ and _ApplyUpdates_ in [stateustoredb.go](core/ledger/kvledger/txmgmt/statedb/stateustoredb/stateustoredb.go). 

## Makefile
* Previously, the _peer_ process is built within _hyperledger/fabric-baseimage_ image
* We additionally specify a new image _hyperledger/fabric-forkbase_, which is based upon _hyperledger/fabric-baseimage_ image. But We include relevant __ForkBase__ dependencies into this new image in order to firstly build the above _db.cc_ and then _peer_ in Fabric# 
* These dynamic dependencies will also get included into _hyperledger/fabric-peer_ image.

# Sample Provenance dependent Contract
* [Smallbank](examples/chaincode/go/smallbank/smallbank.go), which is used to evaluate Fabric#'s performance for Figure 15 in [2]
* [Token](examples/chaincode/go/token/token.go), the introductory example in [2]. 
* [FibonacciYCSB](examples/prov_demo). Refer to [Readme](examples/prov_demo/README.md) to reproduce experiments in Figure 10(a) in [2]

# Forkbase Development
We also provide scripts to reproduce Figure 10(b) and 11(a,b,c) in [2]. 
* Enter into the created Forkbase docker container
```
docker run -it hyperledger/forkbase /bin/bash
```
* Build three executables _lineagechain_, _lineagechainMinus_ and _hypeledgePlus_
``` 
make all;
```
* Invoke `lineagechain bfs`, `lineagechain query` or `lineagechain scan` to reproduce series for LineageChain in Figure 10(b), 11(a,b) and 11(c) respectively.
* The other two executables are used similarly and dedicated for series LineageChain- and Hyperledger+. 
* `make test` will build [test_db.cc](images/forkbase/payload/test/test_db.cc) and generate an executable _test.bin_. It shows the exact Merkle DAG in Figure 6 and Deterministic Append-only Skip list in Figure 8 in [2]. 

# Note
* You will notice plenty of term __ustore__ in the source code and docs. __Ustore__ is our internal name for __Forkbase__. Both are inter-changable. 
* Feel free to leverage on our Fabric# to build provenance-dependent smart contracts. But REFRAIN from using the following two characters, dash(-) and underscore(_) in both key and value for the contract developing and execution (e.g, Never _stub.PutState("abv_def", "123-234)_). They have special treatment in our internal implementation. 