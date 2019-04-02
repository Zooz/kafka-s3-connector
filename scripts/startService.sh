#!/bin/bash -x

if [ -z ${MEM+x} ]; then
	MEM=512
fi

# Heap size is set to 75% of the total RAM given to the docker
HEAP_SIZE=$(echo "($MEM * 0.75) / 1" | bc)

echo HEAP_SIZE=$HEAP_SIZE

# All command-line arguments to be passed to the start script
# All -XX* commands are to better-support JVM memory in Docker.
# See: https://docs.google.com/presentation/d/1ZcJ1w_uaAOX6VRfs-3egIn_PGTbSAVIh9akJQHWAuP8
exec svc/bin/start -J-Xms128M -J-Xmx${HEAP_SIZE}M -J-XX:+UnlockDiagnosticVMOptions -J-XX:+PrintFlagsFinal -J-XX:+UnlockExperimentalVMOptions -J-XX:+UseCGroupMemoryLimitForHeap -J-XX:MaxRAMFraction=2
