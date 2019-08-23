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
    var peer = client.newPeer(peerAddr);
    var blkHeight = "" + info.height.low;
    start = new Date();
    return ccUtil.updateE2E(channel, client, peer, "fupdate", [key, blkHeight, "" + scannedBlkCount]);
}).then(()=>{
    end = new Date();
    var duration = end - start;
    console.log("Total Duration: %d ms for Fupdate function ", duration);
}).catch((err)=>{
    console.log("Err: ", err);
});
