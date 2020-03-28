import sys
import math

def main():
    if len(sys.argv) < 2:
        print "python process_occ-latest_peer.py <peer/log/path>"
        return 1

    scheduleTxnCount = 0
    total_conflict_count = 0
    with open(sys.argv[1]) as fp:
        for line in fp:
            if " # of conflicted txns:" in line:
                splits = line.split()
                total_conflict_count += int(splits[-1])
                
    print "# of conflicted txns: \t", total_conflict_count


if __name__ == "__main__":
    sys.exit(main())