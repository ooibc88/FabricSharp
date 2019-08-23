CHANNEL_NAME=rpcchannel
LANGUAGE=golang
CC_SRC_PATH=ycsb
CC_NAME=ycsb
CC_VERSION=1.0

if [ "$#" -ne 2 ]; then
    echo "Illegal number of parameters"
    echo "Should be ./init.sh <orderer_addr> <peer_addr>"
    exit 1
fi

# ORDER_ADDR=10.0.0.30:7050
# PEER_ADDR=10.0.0.3:7051
ORDER_ADDR=$1
PEER_ADDR=$2
cwd=$(pwd)

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR

if [ -z ${GOPATH+x} ]; then 
  echo "GOPATH is unset. Pls install golang..."; 
fi

# echo "Copy the ycsb chaincode directory under gopath.."
# cp -r ${CC_SRC_PATH} ${GOPATH}/src/${CC_SRC_PATH}

export FABRIC_CFG_PATH=.
export CORE_PEER_ADDRESS=${PEER_ADDR} 
export CORE_PEER_LOCALMSPID=Org1MSP 
export CORE_PEER_MSPCONFIGPATH=./crypto_config/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp 

# Create channel

peer channel create -o $ORDER_ADDR -c $CHANNEL_NAME -f ./channel_artifacts/channel.tx  >&log.txt
if [ $? -eq 0 ]
then
  echo "Successfully create channel ${CHANNEL_NAME}"
else
  echo "Fail to create channel ${CHANNEL_NAME}"
  exit 1
fi

# Join Channel

peer channel join -b ./${CHANNEL_NAME}.block  >&log.txt
if [ $? -eq 0 ]
then
  echo "Successfully join channel ${CHANNEL_NAME}"
else
  echo "Fail to join channel ${CHANNEL_NAME}"
  exit 1
fi

# Install chaincode

peer chaincode install -n ${CC_NAME} -v ${CC_VERSION} -l ${LANGUAGE} -p ${CC_SRC_PATH} >&log.txt
if [ $? -eq 0 ]
then
  echo "Successfully install chaincode ${CC_NAME}"
else
  echo "Fail to install chaincode ${CC_NAME}"
  exit 1
fi

peer chaincode instantiate -o ${ORDER_ADDR} -C ${CHANNEL_NAME} -c '{"Args":["init"]}' -n ${CC_NAME} -l ${LANGUAGE} -v ${CC_VERSION} -P "AND ('Org1MSP.peer')" >&log.txt

if [ $? -eq 0 ]
then
  echo "Successfully instantiate chaincode ${CC_NAME}"
else
  echo "Fail to instantiate chaincode ${CC_NAME}"
  exit 1
fi
cd $cwd