#!/bin/sh
#sample usage : /u01/odi/shb/sld.sh "/u01/odi/shb/Ftxns_RtxnsSh.20151222.unls"
##################################################################################################
if [ $# != 1 ]
then
   echo "Usage: sld file_name"
   exit 1
fi
##################################################################################################
export NLS_DATE_FORMAT='dd/mm/yyyy'
echo "NLS Calendar SET TO : "$NLS_CALENDAR
echo "NLS DATE FORMAT TO : "$NLS_DATE_FORMAT
##################################################################################################
LOAD_DIR="/u01/odi/shb/load"
USER="CBI_SRC"
PASSWORD="ora2200\$\$"
TABLE_NAME="SRC_SHB_ITXN"
FILE_NAME=$1
##################################################################################################
clear
START_TIME=`date`
echo $START_TIME
echo "User Name: " $USER
echo "Table Name: " $TABLE_NAME
echo "File Name: " $FILE_NAME
if [ -d "$LOAD_DIR" ];
then
	rm $LOAD_DIR/* > /dev/null 2>&1
else
	mkdir $LOAD_DIR
fi
##################################################################################################
FILE_SIZE=`stat -c %s $FILE_NAME`
echo "File Size: " $FILE_SIZE
PIECE_SIZE=`expr $FILE_SIZE / 32`
PIECE_SIZE=`expr $PIECE_SIZE / 1048576`
echo "Piece Size in Mega Bytes: " $PIECE_SIZE
echo "Number of Pieces: " `expr $FILE_SIZE / $PIECE_SIZE / 1048576`
split -C $PIECE_SIZE"m" $FILE_NAME $LOAD_DIR/data_ --numeric-suffixes --suffix-length=3
for f in `ls $LOAD_DIR/data_*`
do
  mv $f $f.dat
done
##################################################################################################
sqlplus -s $USER/$PASSWORD >/dev/null 2>&1 << EOF
set head off
set echo off
set feed off
set termout off
set verify off
set pages 1000
spool $LOAD_DIR/$TABLE_NAME.lst
truncate table $TABLE_NAME;
select column_name || ' ":' || column_name || '",' from user_tab_columns where table_name = upper('$TABLE_NAME') order by column_id;
spool off
exit;
EOF
##################################################################################################
echo "OPTIONS (BINDSIZE=256000, DIRECT=TRUE, PARALLEL=TRUE, MULTITHREADING=TRUE, readsize=10485760,SKIP_INDEX_MAINTENANCE=TRUE,SKIP_UNUSABLE_INDEXES=TRUE,ERRORS=31794883, SILENT=(FEEDBACK,ERRORS))" > $LOAD_DIR/ctl.tmpl
echo "UNRECOVERABLE" >> $LOAD_DIR/ctl.tmpl
echo "LOAD DATA" >> $LOAD_DIR/ctl.tmpl
echo "INFILE '$LOAD_DIR/xfnamex'" >> $LOAD_DIR/ctl.tmpl
echo "BADFILE '$LOAD_DIR/xfnamex.bad'" >> $LOAD_DIR/ctl.tmpl
echo "DISCARDFILE '$LOAD_DIR/xfnamex.discard'" >> $LOAD_DIR/ctl.tmpl
echo " " >> $LOAD_DIR/ctl.tmpl
echo "APPEND" >> $LOAD_DIR/ctl.tmpl
echo "INTO TABLE $USER.$TABLE_NAME" >> $LOAD_DIR/ctl.tmpl
echo "APPEND" >> $LOAD_DIR/ctl.tmpl
echo "REENABLE DISABLED_CONSTRAINTS" >> $LOAD_DIR/ctl.tmpl
echo "FIELDS TERMINATED BY '|'  " >> $LOAD_DIR/ctl.tmpl
echo "TRAILING NULLCOLS" >> $LOAD_DIR/ctl.tmpl
echo "(" >> $LOAD_DIR/ctl.tmpl
cat $LOAD_DIR/$TABLE_NAME.lst >> $LOAD_DIR/ctl.tmpl
sed "$ s/,//" $LOAD_DIR/ctl.tmpl > $LOAD_DIR/ctl1.tmpl
mv $LOAD_DIR/ctl1.tmpl $LOAD_DIR/ctl.tmpl
echo ")" >> $LOAD_DIR/ctl.tmpl

for f in `ls $LOAD_DIR/data_*`
do
  sed "s/xfnamex/$(basename $f)/g" $LOAD_DIR/ctl.tmpl > $f.ctl
  nohup sqlldr $USER/$PASSWORD  control=$f.ctl log=$f.log >/dev/null 2>&1 &
done

echo "Please wait, Loading..."
while [ `ps | grep -i sqlldr | grep -v grep | wc -l` -ne 0 ]; do
  sleep 1
done
##################################################################################################
END_TIME=`date`
echo $END_TIME
NOW=$(date +"%Y%m%d-%H%M")
FILE="$TABLE_NAME"_"$NOW".log
# rest of script
cat $LOAD_DIR/*.log |egrep '^Table|Rows success|Rows not loaded' |grep -v 'loaded from' >> /home/oracle/$FILE
cat /home/oracle/$FILE
##################################################################################################