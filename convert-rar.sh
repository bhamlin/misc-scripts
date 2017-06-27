#!/bin/bash

CMD_RAR='/usr/bin/rar'
CMD_TAR='/bin/tar'
CMD_XZ='/usr/bin/xz'

rm -rf bills

for rar_file; do
    base_file=`basename ${rar_file} .rar`
    echo Operating on ${base_file}
    mkdir bills
    cd bills
        echo -n 'Extracting bills...'
        ${CMD_RAR} x ../${rar_file} > /dev/null
        echo -n ' Compiling bills...'
        ${CMD_TAR} cf ../${base_file}.tar *
        echo ' Done!'
        cd ..
    rm -rf bills &
    ${CMD_XZ} -9ev ${base_file}.tar
done
