'use strict';

const ccName = "ycsb"
const ccUtil = require("./ccutil.js")

function getRandomInt(max) {
    return Math.floor(Math.random() * Math.floor(max));
}

// Update 100 keys with random integer value concurrently. 
function invokeTxns(channel, client) {
    return Promise.resolve().then(()=>{
        var endorsePromises = [];
        for (var i = 0;i < 100; i++) {
            let txIdObject = client.newTransactionID();
            let key = "key" + i;
            let value = "" + getRandomInt(100);
            const proposalRequest = {
                chaincodeId: ccName,
                fcn: "update",
                args: [key, value],
                txId: txIdObject,
            }
            endorsePromises.push(channel.sendTransactionProposal(proposalRequest));

        }
        return Promise.all(endorsePromises);
    }).then((endorsedResults)=>{
        var orderPromises = [];
        endorsedResults.forEach((results)=>{
            var proposalResponses = results[0];
            var proposal = results[1];
            if (proposalResponses && proposalResponses[0].response && 
                proposalResponses[0].response.status === 200){
                // console.log('Transaction proposal was good');
            } else {
                console.log(proposalResponses)
                throw new Error('Invalid Proposal');
            }

            var request = { proposalResponses: proposalResponses, proposal: proposal };

            var orderPromise = channel.sendTransaction(request);
            orderPromises.push(orderPromise);
        });
        return Promise.all(orderPromises);
    });
}

// const ordererAddr = "grpc://10.0.0.30:7050"
// const peerAddr = "grpc://10.0.0.3:7051"
if (process.argv.length < 4) {
    console.log("Invalid paramater...");
    console.log("Should be 'node load.js <ordererAddr> <peerAddr>. '");
    process.exit(1);

}

var ordererAddr = "grpc://" + process.argv[2];
var peerAddr = "grpc://" + process.argv[3];
var channel;
var client;
var roundCount = 100;

console.log("Start to create the channel...");
Promise.resolve().then(()=>{
    return ccUtil.createChannelAndClient(peerAddr, ordererAddr);
}).then((result)=>{
    channel = result.channel;
    client = result.client;
    var rounds = [];
    for (var i = 0;i < roundCount; i++) {
        rounds.push(i);
    }
    console.log("Start to load data...");
    // Trigger invokeTxns() 100 rounds. Each round will load 100 keys with random value.
    // Wait for 1s before the next round.  
    return rounds.reduce((prev, roundIdx)=>{
        return prev.then(()=>{
            console.log("  Start round " + roundIdx);
            return invokeTxns(channel, client);
        }).then(()=>{
            // console.log("Sleep for 500ms...");
            return new Promise(resolve => setTimeout(resolve, 500));
        });
    }, Promise.resolve());
}).then(()=>{
    return channel.queryInfo();
}).then((info)=>{
    console.log("Current Chain Height: " + info.height.low);
}).catch((err)=>{
    console.log("Err: ", err);
});