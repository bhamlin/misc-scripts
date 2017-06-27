#!/bin/bash

TEMP_TARGET=/tmp/pbills
FTP_HOST=ftp.cepci.org

mkdir -p $TEMP_TARGET

rsync -vt "/mnt/engineering/Power Bills/"* "$TEMP_TARGET/"
/usr/bin/ncftpget -u 'bla-bill' -p '[REDACTED]' $FTP_HOST '/tmp/pbills/' '*'
rsync -vt "$TEMP_TARGET/"* "/mnt/engineering/Power Bills/"

rm -rf $TEMP_TARGET
