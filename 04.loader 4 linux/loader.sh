readonly COLOR_RED='\033[0;31m'
readonly COLOR_NC='\033[0m' #No Color
readonly COLOR_YELLOW='\033[1;33m'
readonly COLOR_BLUE='\033[1;34m'

readonly DB_USER=CBI_SRC
readonly DB_PASS=ora2200\$\$
readonly DB_IP=172.16.3.10
readonly DB_PORT=2483
readonly DB_SID=cbidw
#======================================================
TXTFILE=$1
TABLE="CBI_SRC.SRC_SHB_ITXN"
#======================================================
sqlplus -s $DB_USER/$DB_PASS@$DB_IP:$DB_PORT/$DB_SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
truncate table $TABLE; 
exit; 
END_OF_SQL
#======================================================
sqlldr $DB_USER/$DB_PASS control=/source/shb/sqlldr_ctrl.txt log=/source/shb/sqlldr_log.txt data=$TXTFILE
#======================================================
TXTFILE_LENGTH=$(wc -l < $TXTFILE)
printf "${COLOR_BLUE}TXTFILE=${COLOR_YELLOW}$TXTFILE\n${COLOR_NC}"
printf "${COLOR_BLUE}TXTFILE_LENGTH=${COLOR_YELLOW}$TXTFILE_LENGTH\n${COLOR_NC}"
#======================================================
TABLE_LENGTH=$(sqlplus -s $DB_USER/$DB_PASS@$DB_IP:$DB_PORT/$DB_SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from $TABLE; 
exit; 
END_OF_SQL
)
printf "${COLOR_BLUE}TABLE=${COLOR_YELLOW}$TABLE\n${COLOR_NC}"
printf "${COLOR_BLUE}TABLE_LENGTH=${COLOR_YELLOW}$TABLE_LENGTH\n${COLOR_NC}"