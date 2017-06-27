OUTPUT_FILE=/tmp/temp_data

MAX_TIME=`mysql --login-path=warehouse-admin --batch --raw -D meters -e "select max(time) from w_sumter" | tail -n 1`

psql -h 10.50.10.208 -d "Black River" -U postgres -o $OUTPUT_FILE -A -F , -t -c \
    "select measurement_time, temperature from weather.sumter_data where measurement_time > '$MAX_TIME'"

awk -F , "{print \"insert ignore into w_sumter (time, temp_f) values ('\" \$1 \"', \" \$2 \");\"}" $OUTPUT_FILE | \
    mysql --login-path=warehouse-admin -D meters

rm $OUTPUT_FILE
