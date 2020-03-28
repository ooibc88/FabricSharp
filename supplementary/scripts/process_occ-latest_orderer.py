import sys
import math

def main():
    if len(sys.argv) < 2:
        print "python process_occ-latest_orderer.py <order/log/path>"
        return 1

    scheduleTxnCount = 0
    total_drop_count = 0
    total_blk_process = 0
    total_dep_resolution = 0
    total_prune_filter = 0
    blk_count = 0
    with open(sys.argv[1]) as fp:
        for line in fp:
            if "Process Block: " in line:
                splits = line.split()
                total_drop_count += int(splits[-1])
                total_prune_filter += int(splits[-7])
                total_dep_resolution += int(splits[-11])
                total_blk_process += int(splits[-15])
                blk_count += 1
                # print splits
                # return
                
    print "# of dropped txns: \t", total_drop_count
    print "Blk process duration(us): \t", total_blk_process / blk_count
    print "\tDep resolution duration: \t", total_dep_resolution / blk_count
    print "\tPrune_filter duration: \t", total_prune_filter / blk_count


if __name__ == "__main__":
    sys.exit(main())