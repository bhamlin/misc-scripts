UMOUNT_BIN=/bin/umount
MOUNT_BIN=/bin/mount
GREP_BIN=/bin/grep
SUDO_BIN=/usr/bin/sudo

for MOUNT in `$MOUNT_BIN | $GREP_BIN -oP '/mnt/(engineering|network)'`; do
    echo -n "Unmounting $MOUNT... "
    $SUDO_BIN $UMOUNT_BIN -f $MOUNT
    echo "Done!"
done

echo -n "Mounting /mnt/network... "
$SUDO_BIN $MOUNT_BIN '//10.50.10.5/network'     '/mnt/network'     -o 'uid=bhamlin,gid=users,user=bhamlin,password=andislan'
echo "Done!"
echo -n "Mounting /mnt/engineering... "
$SUDO_BIN $MOUNT_BIN '//10.50.10.5/engineering' '/mnt/engineering' -o 'uid=bhamlin,gid=users,user=bhamlin,password=andislan'
echo "Done!"
