import sys
import math

def main():
    if len(sys.argv) < 2:
        print "python process_occ-sharp_orderer.py <order/log/path>"
        return 1

    scheduleTxnCount = 0
    abortTxnCount = 0
    staleTxnCount = 0
    desertedTxnCount = 0

    totalProcessTxnDelay = 0
    totalTxnResolveDependencyDelay = 0
    totalTxnBfsTestAccessibilityDelay = 0
    totalTxnBfsTraversalCount = 0
    totalTxnUpdatePendingDelay = 0

    blkCount = 0
    totalProcessBlkDelay = 0
    totalInBlkTxnSpan = 0
    totalBFSConcernedTxnDelay = 0
    totalTopoSortDelay = 0
    totalResolvePendingForWwDelay = 0
    totalStorageDelay = 0
    totalTopoTraverseWwTxnDelay = 0
    totalTxnQPruningDelay = 0
    totalAntiDependentTxnCount = 0

    totalTopoTraversalCount = 0
    totalPrunedTxnCount = 0
    totalConcernedTxnCount = 0
    with open(sys.argv[1]) as fp:
        for line in fp:
            splits = line.split()
            if "Schedule Txn " in line:
                scheduleTxnCount += 1
                totalTxnUpdatePendingDelay += int(splits[-2])
                totalTxnBfsTestAccessibilityDelay +=int(splits[-6])
                bfsTraversalCount = int(splits[-8])
                totalTxnBfsTraversalCount += bfsTraversalCount
                if bfsTraversalCount > 1:
                    totalAntiDependentTxnCount += 1
                totalTxnResolveDependencyDelay += int(splits[-15])
                totalProcessTxnDelay += int(splits[-19])
            elif "Drop Txn " in line:
                staleTxnCount += 1
            elif "Abort Txn " in line:
                abortTxnCount += 1
            elif "DESERT Txn " in line:
                desertedTxnCount += 1
            elif "Retrieve Schedule" in line:
                blkCount += 1
                totalProcessBlkDelay += int(splits[-2])
                txnSpan = float(splits[-5])
                if not math.isnan(txnSpan):
                    totalInBlkTxnSpan = totalInBlkTxnSpan + txnSpan
            elif "Compute schedule from topo sort" in line:
                totalTopoSortDelay += int(splits[-2])
            elif "Resolve pending txns in " in line:
                totalResolvePendingForWwDelay += int(splits[-2])
            elif "Store committed keys in " in line:
                totalStorageDelay += int(splits[-2])
            elif "txns to update ww dependency in"  in line:
                totalTopoTraverseWwTxnDelay += int(splits[-2])
                totalTopoTraversalCount += int(splits[-9])
            elif "non-concerned txns from TxnPQueue" in line:
                totalTxnQPruningDelay += int(splits[-2])
                totalPrunedTxnCount += int(splits[-8])
            elif "# of Concerned txns in the queue" in line:
                totalConcernedTxnCount += int(splits[-1])
                
    print "Process Txn Delay (us)\t", totalProcessTxnDelay / scheduleTxnCount
    print "\t", totalTxnResolveDependencyDelay / scheduleTxnCount,"\tResolve Dependency"
    print "\t", totalTxnBfsTestAccessibilityDelay / scheduleTxnCount,"\tBFS Traversal to test accessibility"
    print "\t", totalTxnUpdatePendingDelay / scheduleTxnCount,"\tUpdate into Pending txn (us)"
    print "Average BFS traversal to test for txn cycle:\t", totalTxnBfsTraversalCount / scheduleTxnCount
    print "# of ordered txn: \t", staleTxnCount + abortTxnCount + desertedTxnCount + scheduleTxnCount
    print "\t", scheduleTxnCount, "\t scheduled"
    print "\t\t", totalAntiDependentTxnCount, "\t (anti-dependent)"
    print "\t", abortTxnCount, "\t aborted"
    print "\t", staleTxnCount, "\t dropped due to too early snapshot"
    print "\t", desertedTxnCount, "\t deserted due to bloom filter false positive"
    print "Process Blk Delay (us)\t", totalProcessBlkDelay / blkCount
    print "\t", totalTopoSortDelay / blkCount, "\t Toposort txns for schedule "
    print "\t", totalResolvePendingForWwDelay / blkCount, "\t Resolve pendings txns"
    print "\t", totalStorageDelay / blkCount, "\t Store committed keys and txns"
    print "\t", totalTopoTraverseWwTxnDelay / blkCount, "\t Topo traverse for ww dependency"
    print "\t", totalTxnQPruningDelay / blkCount, "\t Prune non-concerned txns from Txn Queue"
    print "avg # of concerned txns in queue\t", totalConcernedTxnCount / blkCount
    print "avg # of non-concerned removed txns\t", totalPrunedTxnCount / blkCount
    print "avg # of topo-sorted traversal\t", totalTopoTraversalCount / blkCount
    print "# of blk\t", blkCount
    print "avg txn span\t", round(totalInBlkTxnSpan / blkCount, 2)
    
    

if __name__ == "__main__":
    sys.exit(main())