import sys
import math

def main():
    if len(sys.argv) < 2:
        print "python process_occ-standard_orderer.py <order/log/path>"
        return 1

    total_schedule_count = 0
    total_drop_count = 0
    cww_drop_count = 0 
    middle_drop_count = 0
    rw_drop_count = 0
    anti_drop_count = 0
    total_txn_delay = 0
    txn_count = 0 
    with open(sys.argv[1]) as fp:
        for line in fp:
            if "Process Txn:" in line:
                txn_count += 1
                if "Schedule Txn" in line:
                    total_schedule_count += 1
                elif "CWW-Abort Txn" in line:
                    cww_drop_count += 1
                elif "AntiRw-Abort Txn" in line:
                    anti_drop_count += 1
                elif "RW-Abort Txn" in line:
                    rw_drop_count += 1
                elif "Middle-Abort Txn" in line:
                    middle_drop_count += 1

                splits = line.split()
                total_txn_delay += int(splits[-2])
                # print int(splits[-2])
                # print total_txn_delay
                # return
    
    total_drop_count = middle_drop_count + rw_drop_count + anti_drop_count + cww_drop_count
    print "total # of scheduled txns: \t", total_schedule_count
    print "total # of dropped txns: \t", total_drop_count
    print "\t# of dropped txns due to cww: \t", cww_drop_count
    print "\t# of dropped txns due to bridge rw and anti-rw: \t", middle_drop_count
    print "\t# of dropped txns due to from-rw txns tagged with ANTI_RW_IN: \t", rw_drop_count
    print "\t# of dropped txns due to to-anti-rw txns tagged with ANTI_RW_OUT/RW_OUT: \t", anti_drop_count
    print "Txn process duration(us): \t", total_txn_delay / txn_count


if __name__ == "__main__":
    sys.exit(main())