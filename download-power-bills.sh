#!/bin/bash

TEMP_TARGET=/tmp/pbills
FTP_HOST=ftp.cepci.org

mkdir -p $TEMP_TARGET

rsync -t --progress "/mnt/engineering/Power Bills/"* "$TEMP_TARGET/"
/usr/bin/ncftpget -u 'bla-bill' -p '[REDACTED]' $FTP_HOST '/tmp/pbills/' '*'
rsync -t --progress "$TEMP_TARGET/"* "/mnt/engineering/Power Bills/"

rm -rfv $TEMP_TARGET
