// 'use strict';


const ccName = "ycsb"
const path = require('path');
const fs = require('fs');
const os = require('os')

const Client = require('fabric-client');

const client = new Client();
const keyPath = path.join(__dirname, "crypto_config/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp/keystore/d63762bc9a323ac14374410b836e4daf9859ed1c225e64aa6369eb18cd531fa5_sk")
const certPath = path.join(__dirname, "crypto_config/peerOrganizations/org1.example.com/users/Admin@org1.example.com/msp/signcerts/Admin@org1.example.com-cert.pem")
const MSPID = "Org1MSP"
const channelName = "rpcchannel"
var txIdObject;
module.exports.ccName = ccName;

module.exports.createChannelAndClient = function (peerAddr, ordererAddr) {
    return Promise.resolve().then(()=>{
        console.log("Set Cryptosuite and keystore")
        const cryptoSuite = Client.newCryptoSuite();
        const tmpPath = path.join(os.tmpdir(), 'hfc')
        cryptoSuite.setCryptoKeyStore(Client.newCryptoKeyStore({path: tmpPath}));
        client.setCryptoSuite(cryptoSuite);
        return Client.newDefaultKeyValueStore({path: tmpPath});
    }).then((store)=>{
        console.log("Create the store...")
        if (store) {client.setStateStore(store); }
        const keyPEM = fs.readFileSync(keyPath);
        const certPEM = fs.readFileSync(certPath);
        const createUserOpt = { 
            username: "Org1Peer", 
            mspid: MSPID, 
            cryptoContent: { 
                privateKeyPEM: keyPEM.toString(), 
                signedCertPEM: certPEM.toString() 
            } 
        };
        return client.createUser(createUserOpt);
    }).then((user)=>{
        console.log("Create the user...")
        let channel = client.newChannel(channelName);
        let peer = client.newPeer(peerAddr);
        channel.addPeer(peer);
        var orderer = client.newOrderer(ordererAddr); 
        
        channel.addOrderer(orderer);
        channel.initialize();
        return {channel: channel, client: client};
    }).catch((err)=>{
        console.log("Err: ", err);
    });
}

module.exports.updateE2E = function(channel, client, peer, functionName, args) {
    return Promise.resolve().then(()=>{
        txIdObject = client.newTransactionID();
        const proposalRequest = {
            chaincodeId: ccName,
            fcn: functionName,
            args: args,
            txId: txIdObject,
        }
        return channel.sendTransactionProposal(proposalRequest);

    }).then((results)=>{
        var promises = [];
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

        var sendPromise = channel.sendTransaction(request);
        promises.push(sendPromise); 

		let eventHub = channel.newChannelEventHub(peer);
        let txIdStr = txIdObject.getTransactionID();
		let txPromise = new Promise((resolve, reject) => {
			let handle = setTimeout(() => {
				eventHub.unregisterTxEvent(txIdStr);
				eventHub.disconnect();
				resolve({eventStatus : 'TIMEOUT'}); 
			}, 3000);
			eventHub.registerTxEvent(txIdStr, (tx, code) => {
				clearTimeout(handle);
				var returnStatus = {eventStatus : code, txId : txIdStr};
				if (code !== 'VALID') {
					console.error('The transaction was invalid, code = ' + code);
					resolve(returnStatus); 
				} else {
					console.log('The transaction has been committed on peer ' + eventHub.getPeerAddr());
					resolve(returnStatus);
				}
			}, (err) => {
				//this is the callback if something goes wrong with the event registration or processing
				reject(new Error('There was a problem with the eventhub ::'+err));
			},
				{disconnect: true} //disconnect when complete
			);
			eventHub.connect();

		});
		promises.push(txPromise);
        return Promise.all(promises);
    }).then((results)=>{
        if (results && results[0] && results[0].status === 'SUCCESS') {
            console.log('Successfully sent transaction to the orderer.');
        } else {
            console.error('Failed to order the transaction. Error code: ' + results[0].status);
        }
    
        if(results && results[1] && results[1].eventStatus === 'VALID') {
            console.log('Successfully committed the change to the ledger by the peer');
        } else {
            console.log('Transaction failed to be committed to the ledger due to ::'+results[1].eventStatus);
        }
    });
}