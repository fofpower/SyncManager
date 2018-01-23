#!/usr/bin/env bash
echo "Sync Job start!"

DROP_DELETE='off'

CONCURRENCY=4

PROCESS=4

RECORD_LIMIT=2000

KEY_LIMIT=50


cd #填写config目录路径

smyt_sync_task -a update -s product -c ${CONCURRENCY} -rl ${RECORD_LIMIT} -kl ${KEY_LIMIT} -p ${PROCESS} -d ${DROP_DELETE}

smyt_sync_task -a update -s product_mutual -c ${CONCURRENCY} -rl ${RECORD_LIMIT} -kl ${KEY_LIMIT} -p ${PROCESS} -d ${DROP_DELETE}

echo "Sync Job done!"
