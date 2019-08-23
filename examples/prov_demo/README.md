# Overview
This short demo shows show Fabric# enables a provenance-dependent contract, in which the updated value for a key is dependent on its previous versions within a specified number of earlier blocks. (We refer it as FibonacciUpdate)

# Procedure
## Spin up the network 
* The network shall consist a single order and a peer. 
* The network shall use the provided channel_artifacts and crypto_config materials. 

## Setup
* Create, join channel & Install, instantiate chaincode
```
./init.sh <orderer_addr> <peer_addr>
```

## Load Data
```
node load.js <orderer_addr> <peer_addr>
```

## With Provenanec-aware APIs in Fabric#
```
node newFUpdate.js <orderer_addr> <peer_addr> <# of earlier blocks>
```

## Without Provenanec-aware APIs
* Achieve it via linear block scanning. 
```
node oldFUpdate.js <orderer_addr> <peer_addr> <# of earlier blocks>
```
