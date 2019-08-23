'use strict';

const ccUtil = require("./ccutil.js")



// const ordererAddr = "grpc://10.0.0.30:7050"
// const peerAddr = "grpc://10.0.0.3:7051"
if (process.argv.length < 5) {
    console.log("Invalid paramater...");
    console.log("Should be 'node load.js <ordererAddr> <peerAddr> <scannedBlkCount> '");
    process.exit(1);

}

var ordererAddr = "grpc://" + process.argv[2];
var peerAddr = "grpc://" + process.argv[3];
var scannedBlkCount = parseInt(process.argv[4]);
var channel;
var client;
var sum = 0;
var start, end;
const key = "key0";  // any key is fine. 

console.log("Start to create the channel...");
Promise.resolve().then(()=>{
    return ccUtil.createChannelAndClient(peerAddr, ordererAddr);
}).then((result)=>{
    channel = result.channel;
    client = result.client;
}).then(()=>{
    return channel.queryInfo();
}).then((info)=>{
    console.log("Current Chain Height: " + info.height.low);
    // Ignore blk 0, as this is the config block in Fabric
    let startBlkNum = 1;
    if (scannedBlkCount < info.height.low) {
        startBlkNum = info.height.low - scannedBlkCount;
    }
    var startIdxs = [];
    for (var i = info.height.low - 1; i >= startBlkNum; i--) {
        startIdxs.push(i);
    }
    
    start = new Date();
    return startIdxs.reduce((prev, blkIdx)=>{
        return prev.then(()=>{
                console.log("Start to fetch blk " + blkIdx);
                return channel.queryBlock(blkIdx);
            }).then((blk)=>{
                let value = 0;
                let txnCount = blk.data.data.length;
                for (var i = 0;i < txnCount; i++) {
                    let txnData = blk.data.data[i];
                    txnData.payload.data.actions.forEach((actionData)=>{
                        actionData.payload.action.proposal_response_payload.extension.results.ns_rwset.forEach((nsData)=>{
                            if (nsData.namespace == ccUtil.ccName) {
                                nsData.rwset.writes.forEach((write)=>{
                                    if (write.key == key) {
                                        value = parseInt(write.value);
                                    }
                                });
                            }  // end if 
                        });  // end nsData
                    });  // end txnData
                }  // end for txnCount
                // console.log("Updated value for key " + key + " is " + value + " at blk " + blkIdx);
                sum += value;
            });
    }, Promise.resolve());
}).then(()=>{
    var peer = client.newPeer(peerAddr);
    var sumStr = "" + sum;
    return ccUtil.updateE2E(channel, client, peer, "update", [key, sumStr]);
}).then(()=>{
    end = new Date();
    var duration = end - start;
    console.log("Total Duration: %d ms with updated value %d for key ", duration, sum, key);
}).catch((err)=>{
    console.log("Err: ", err);
});
