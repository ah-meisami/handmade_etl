#################################################################################
readonly COLOR_RED='\033[0;31m'
readonly COLOR_NC='\033[0m' #No Color 
readonly COLOR_YELLOW='\033[1;33m' 
readonly COLOR_BLUE='\033[1;34m' 
#################################################################################
readonly USER=bts
readonly PASS=btspa\$\$
readonly IP=172.16.3.40
readonly PORT=2483
readonly SID=DW
#################################################################################
function checkFileExist(){
	if [ ! -e $1 ]; then
		printf "${COLOR_RED}File not found: ${COLOR_YELLOW}$1\n${COLOR_NC}"
		exit 1
	fi
}
SUCCESS_COUNTER=0
function checkFileLengthWithDBTable(){
	if [ $1 == $2 ] && [ $1 != 0 ]; then
	  SUCCESS_COUNTER=$(($SUCCESS_COUNTER + 1))
	  printf "success $3 ${COLOR_BLUE}($1)\n${COLOR_NC}" 
	else
	  printf "${COLOR_RED}failure $3 ${COLOR_YELLOW}($1 <> $2)\n${COLOR_NC}"
	fi
}
function executeOracleDeltaPackage(){
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL
set pagesize 0 
set newpage 0 
set space 0 
set linesize 1000 
--set echo off 
--set feedback off 
--set verify off 
--set heading off 
--set termout off 
--set trimout on 
--set trimspool on 
set colsep ' '
-----------------------------
execute dlt_pkg.init($1);
-----------------------------
execute dlt_pkg.gnl1_report_crnt_prvs_tables($1);
execute dlt_pkg.exec_etl_processflow($1,'DELTA_GNL');
execute dlt_pkg.gnl2_report_and_fix_temp($1);
execute dlt_pkg.exec_etl_processflow($1,'COMPLETE_GNL','GATHER_STAT=FALSE,PASS_THROUGH_REJECT=FALSE');
execute dlt_pkg.gnl3_report_dimension($1);  
-----------------------------
execute dlt_pkg.trn1_init($1);  
execute dlt_pkg.exec_etl_processflow($1,'SUBPROC_TRN','GATHER_STAT=FALSE,EXEC_TYPE=2,PASS_THROUGH_REJECT=FALSE');
execute dlt_pkg.trn2_report_fact($1);        
-----------------------------
execute dlt_pkg.bln1_init($1);  
execute dlt_pkg.exec_etl_processflow($1,'PROC_BLNC','GATHER_STAT=FALSE,PASS_THROUGH_REJECT=FALSE');
execute dlt_pkg.bln2_report_fact($1);      
-----------------------------
execute dlt_pkg.exec_etl_processflow($1,'PFD_CHQ_CMPL','GATHER_STAT=FALSE,PASS_THROUGH_REJECT=FALSE,P_DATE_OPR=$1');
-----------------------------
execute dlt_pkg.finalize($1);       
-----------------------------
select log_id||'-'||log_mode||'('||log_tag||')'||log_mesg result from dlt_log_tab where date_opr=$1 order by log_id;
-----------------------------
END_OF_SQL
}
#################################################################################
#####GET PREVIOUS DATE WHICH IS NOT HOLIDAY
#####GET NEXT DATE WHICH IS NOT HOLIDAY
#####in holidays we get CURRATE from previous not holiday and (TR,TRS,CTRINFO) from next not holiday:
#################################################################################
if [ -z $1 ]; then
	printf "${COLOR_RED}Please enter date ${COLOR_YELLOW}(Format=13940524).\n${COLOR_NC}" 
	exit 1
else
	INPUT_PARAMETER_TODAY=$1
fi
if [ -z $2 ]; then
	INPUT_PARAMETER_PRDAY=$1
else
	INPUT_PARAMETER_PRDAY=$2
fi
if [ -z $3 ]; then
	INPUT_PARAMETER_NXDAY=$1
else
	INPUT_PARAMETER_NXDAY=$3
fi
printf "INPUT_PARAMETER_TODAY = ${COLOR_YELLOW}$INPUT_PARAMETER_TODAY\n${COLOR_NC}"
printf "INPUT_PARAMETER_PRDAY = ${COLOR_YELLOW}$INPUT_PARAMETER_PRDAY\n${COLOR_NC}"
printf "INPUT_PARAMETER_NXDAY = ${COLOR_YELLOW}$INPUT_PARAMETER_NXDAY\n${COLOR_NC}"
#################################################################################
# read me
# pay attention we load two files in one table (TXTFILE_TRN_SHIBA_BMI,TXTFILE_TRN_SHIBA_BSI --> temp_trn_shiba).
#
#################################################################################
TXTFILE_GNL_BRANCH=/home/deltausr/dataload/GNL/${INPUT_PARAMETER_TODAY}/BTS_GNL_BRANCH_${INPUT_PARAMETER_TODAY}
TXTFILE_GNL_USER=/home/deltausr/dataload/GNL/${INPUT_PARAMETER_TODAY}/BTS_GNL_USER_${INPUT_PARAMETER_TODAY}
TXTFILE_GNL_CUSTADDR=/home/deltausr/dataload/GNL/${INPUT_PARAMETER_TODAY}/BTS_GNL_CUSTADDR_${INPUT_PARAMETER_TODAY}
TXTFILE_GNL_CUSTOMER=/home/deltausr/dataload/GNL/${INPUT_PARAMETER_TODAY}/BTS_GNL_CUSTOMER_${INPUT_PARAMETER_TODAY}
TXTFILE_GNL_CUSTPART=/home/deltausr/dataload/GNL/${INPUT_PARAMETER_TODAY}/BTS_GNL_CUSTPART_${INPUT_PARAMETER_TODAY}
TXTFILE_GNL_ACCOUNT=/home/deltausr/dataload/GNL/${INPUT_PARAMETER_TODAY}/BTS_GNL_ACCOUNT_${INPUT_PARAMETER_TODAY}
TXTFILE_GNL_CODE=/home/deltausr/dataload/GNL/${INPUT_PARAMETER_TODAY}/BTS_GNL_CODE_${INPUT_PARAMETER_TODAY}

TXTFILE_TRN_TRANSACTION=/home/deltausr/dataload/TRN/${INPUT_PARAMETER_TODAY}/BTS_TRN_TRANSACTION_${INPUT_PARAMETER_TODAY}.dat
TXTFILE_TRN_TRNINFO=/home/deltausr/dataload/TRN/${INPUT_PARAMETER_TODAY}/BTS_TRN_TRNINFO_${INPUT_PARAMETER_TODAY}.TXT
TXTFILE_TRN_CTRINFO=/home/deltausr/dataload/TRN/${INPUT_PARAMETER_TODAY}/BTS_TRN_CTRINFO_NEW_${INPUT_PARAMETER_NXDAY}.TXT 
TXTFILE_TRN_TR=/home/deltausr/dataload/TRN/${INPUT_PARAMETER_TODAY}/BTS_TRN_TR_${INPUT_PARAMETER_NXDAY}
TXTFILE_TRN_TRS=/home/deltausr/dataload/TRN/${INPUT_PARAMETER_TODAY}/BTS_TRN_TRS_${INPUT_PARAMETER_NXDAY}.TXT
TXTFILE_TRN_SHIBA_BMI=/home/deltausr/dataload/TRN/${INPUT_PARAMETER_TODAY}/BTS_TRN_SHIBA_BMI2BTS_${INPUT_PARAMETER_NXDAY}.TXT
TXTFILE_TRN_SHIBA_BSI=/home/deltausr/dataload/TRN/${INPUT_PARAMETER_TODAY}/BTS_TRN_SHIBA_BSI2BTS_${INPUT_PARAMETER_NXDAY}.TXT

TXTFILE_BLN_BALANCE=/home/deltausr/dataload/TRN/${INPUT_PARAMETER_TODAY}/BTS_TRN_BALANCE_${INPUT_PARAMETER_TODAY} 
TXTFILE_BLN_CURRATE=/home/deltausr/dataload/TRN/${INPUT_PARAMETER_TODAY}/BTS_TRN_CURRATE_${INPUT_PARAMETER_PRDAY}.TXT

TXTFILE_CHQ_QCCH=/home/deltausr/dataload/CHQ/${INPUT_PARAMETER_TODAY}/BTS_CHQ_CHEQUEQCCH_${INPUT_PARAMETER_TODAY}.TXT
TXTFILE_CHQ_BOOK=/home/deltausr/dataload/CHQ/${INPUT_PARAMETER_TODAY}/BTS_CHQ_CHEQUEBOOK_${INPUT_PARAMETER_TODAY}
TXTFILE_CHQ_PRESS=/home/deltausr/dataload/CHQ/${INPUT_PARAMETER_TODAY}/BTS_CHQ_CHEQUEPRESS_${INPUT_PARAMETER_TODAY}
TXTFILE_CHQ_RETURN=/home/deltausr/dataload/CHQ/${INPUT_PARAMETER_TODAY}/BTS_CHQ_CHEQUERETURN_${INPUT_PARAMETER_TODAY}
TXTFILE_CHQ_STOP=/home/deltausr/dataload/CHQ/${INPUT_PARAMETER_TODAY}/BTS_CHQ_CHEQUESTOP_${INPUT_PARAMETER_TODAY}

TXTFILE_CRD_ITXN=/home/deltausr/dataload/CRD/${INPUT_PARAMETER_TODAY}/BTS_CRD_ITXN_${INPUT_PARAMETER_TODAY}.dat
#################################################################################
checkFileExist $TXTFILE_GNL_BRANCH
checkFileExist $TXTFILE_GNL_USER
checkFileExist $TXTFILE_GNL_CUSTADDR
checkFileExist $TXTFILE_GNL_CUSTOMER
checkFileExist $TXTFILE_GNL_CUSTPART
checkFileExist $TXTFILE_GNL_ACCOUNT
checkFileExist $TXTFILE_GNL_CODE

checkFileExist $TXTFILE_TRN_TRANSACTION
checkFileExist $TXTFILE_TRN_CTRINFO
checkFileExist $TXTFILE_TRN_TR
checkFileExist $TXTFILE_TRN_TRNINFO
checkFileExist $TXTFILE_TRN_TRS
#################################################################################
#ATTENTION : 
#IN HOLIDAYS WE DON'T HAVE SUCH FILES : TRN_SHIBA_BMI , TRN_SHIBA_BSI
#SO BASED ON INPUT PARAMETRS WE DETECT TODAY IS HOLIDAY OR NOT.
#IN HOLIDAYS WE SHOULD NOT CHECK THE EXISTANCE OF THESE FILE.
if [ $INPUT_PARAMETER_TODAY = $INPUT_PARAMETER_PRDAY ]; then
	checkFileExist $TXTFILE_TRN_SHIBA_BMI
	checkFileExist $TXTFILE_TRN_SHIBA_BSI
fi
#################################################################################
checkFileExist $TXTFILE_BLN_BALANCE
checkFileExist $TXTFILE_BLN_CURRATE

checkFileExist $TXTFILE_CHQ_QCCH
checkFileExist $TXTFILE_CHQ_BOOK
checkFileExist $TXTFILE_CHQ_PRESS
checkFileExist $TXTFILE_CHQ_RETURN
checkFileExist $TXTFILE_CHQ_STOP

checkFileExist $TXTFILE_CRD_ITXN
#################################################################################
sed -i s/\|/\ /g $TXTFILE_TRN_TR
sed -i s/\|/\ /g $TXTFILE_TRN_TRS
#################################################################################
LENGTH_TXTFILE_GNL_BRANCH=$(wc -l < $TXTFILE_GNL_BRANCH)
LENGTH_TXTFILE_GNL_USER=$(wc -l < $TXTFILE_GNL_USER)
LENGTH_TXTFILE_GNL_CUSTADDR=$(wc -l < $TXTFILE_GNL_CUSTADDR)
LENGTH_TXTFILE_GNL_CUSTOMER=$(wc -l < $TXTFILE_GNL_CUSTOMER)
LENGTH_TXTFILE_GNL_CUSTPART=$(wc -l < $TXTFILE_GNL_CUSTPART)
LENGTH_TXTFILE_GNL_ACCOUNT=$(wc -l < $TXTFILE_GNL_ACCOUNT)
LENGTH_TXTFILE_GNL_CODE=$(wc -l < $TXTFILE_GNL_CODE)

LENGTH_TXTFILE_TRN_TRANSACTION=$(wc -l < $TXTFILE_TRN_TRANSACTION)
LENGTH_TXTFILE_TRN_CTRINFO=$(wc -l < $TXTFILE_TRN_CTRINFO)
LENGTH_TXTFILE_TRN_TR=$(wc -l < $TXTFILE_TRN_TR)
LENGTH_TXTFILE_TRN_TRNINFO=$(wc -l < $TXTFILE_TRN_TRNINFO)
LENGTH_TXTFILE_TRN_TRS=$(wc -l < $TXTFILE_TRN_TRS)
LENGTH_TXTFILE_TRN_SHIBA_BMI=$(wc -l < $TXTFILE_TRN_SHIBA_BMI)
LENGTH_TXTFILE_TRN_SHIBA_BSI=$(wc -l < $TXTFILE_TRN_SHIBA_BSI)

LENGTH_TXTFILE_BLN_BALANCE=$(wc -l < $TXTFILE_BLN_BALANCE)
LENGTH_TXTFILE_BLN_CURRATE=$(wc -l < $TXTFILE_BLN_CURRATE)

LENGTH_TXTFILE_CHQ_QCCH=$(wc -l < $TXTFILE_CHQ_QCCH)
LENGTH_TXTFILE_CHQ_BOOK=$(wc -l < $TXTFILE_CHQ_BOOK)
LENGTH_TXTFILE_CHQ_PRESS=$(wc -l < $TXTFILE_CHQ_PRESS)
LENGTH_TXTFILE_CHQ_RETURN=$(wc -l < $TXTFILE_CHQ_RETURN)
LENGTH_TXTFILE_CHQ_STOP=$(wc -l < $TXTFILE_CHQ_STOP)

LENGTH_TXTFILE_CRD_ITXN=$(wc -l < $TXTFILE_CRD_ITXN)
#################################################################################
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
truncate table temp_gnl_branch_crnt;
truncate table temp_gnl_user_crnt;
truncate table temp_gnl_custaddr_crnt;
truncate table temp_gnl_custinfo_n;
truncate table temp_gnl_custinfo_crnt;
truncate table temp_gnl_custpart;
truncate table temp_gnl_account_crnt; 
truncate table temp_gnl_bs_sct_crnt;

truncate table std_trn_transaction;
truncate table temp_trn_transaction;
truncate table temp_trn_ctrinfo;
truncate table temp_trn_tr2;
truncate table temp_trn_tr;
truncate table temp_trn_trninfo;
truncate table temp_trn_trs;
truncate table temp_trn_trs2;
truncate table temp_trn_shiba;

truncate table temp_trn_balance;
truncate table temp_trn_rate_currency;

truncate table temp_chq_chequeqcch;
truncate table temp_chq_chequebook;
truncate table temp_chq_chequebook_n;
truncate table temp_chq_chequepress;
truncate table temp_chq_chequepress_n;
truncate table temp_chq_chequereturn;
truncate table temp_chq_chequereturn_n;
truncate table std_chq_chequestop;
truncate table temp_chq_chequestop;
truncate table temp_chq_chequestop_n;

truncate table std_crd_itxn;
END_OF_SQL
#################################################################################
./ldr_gnl_nul $USER $PASS temp_gnl_branch_crnt $TXTFILE_GNL_BRANCH SINGLE 1 \| PERSIAN YYYYMMDD $INPUT_PARAMETER_TODAY
./ldr_gnl_nul $USER $PASS temp_gnl_user_crnt $TXTFILE_GNL_USER SINGLE 1 \| PERSIAN YYYYMMDD $INPUT_PARAMETER_TODAY
./ldr_gnl_nul $USER $PASS temp_gnl_custaddr_crnt $TXTFILE_GNL_CUSTADDR SINGLE 1 \| PERSIAN YYYYMMDD $INPUT_PARAMETER_TODAY
./ldr_gnl_nul $USER $PASS temp_gnl_custinfo_n $TXTFILE_GNL_CUSTOMER SINGLE 1 \| PERSIAN YYYYMMDD $INPUT_PARAMETER_TODAY
./ldr_gnl_nul $USER $PASS temp_gnl_custpart $TXTFILE_GNL_CUSTPART SINGLE 1 \| PERSIAN YYYYMMDD $INPUT_PARAMETER_TODAY
./ldr_gnl_nul $USER $PASS temp_gnl_account_crnt $TXTFILE_GNL_ACCOUNT PARALLEL 4 \| PERSIAN YYYYMMDD $INPUT_PARAMETER_TODAY
./ldr_gnl_nul $USER $PASS temp_gnl_bs_sct_crnt $TXTFILE_GNL_CODE SINGLE 1 \| PERSIAN YYYYMMDD $INPUT_PARAMETER_TODAY  

export NLS_CALENDAR='persian'
export NLS_DATE_FORMAT='yymmdd'
sqlldr $USER/$PASS  data=$TXTFILE_TRN_TRANSACTION  Control=/home/deltausr/trnnew.ctl log=/home/deltausr/logdir/stdtrn.log direct=y
./ldr_nul $USER $PASS temp_trn_ctrinfo $TXTFILE_TRN_CTRINFO SINGLE 1 \| PERSIAN YYMMDD     
./ldr_gnl_nul $USER $PASS temp_trn_tr2 $TXTFILE_TRN_TR SINGLE 1 \| PERSIAN YYMMDD $INPUT_PARAMETER_NXDAY  
sqlldr $USER/$PASS data=$TXTFILE_TRN_TRNINFO Control=/home/deltausr/trninfo.ctl log=/home/deltausr/logdir/trninfo.log direct=y
./ldr_gnl_nul $USER $PASS temp_trn_trs2 $TXTFILE_TRN_TRS SINGLE 1 \ TAB PERSIAN YYMMDD $INPUT_PARAMETER_NXDAY 
./ldr_gnl_nul $USER $PASS temp_trn_shiba $TXTFILE_TRN_SHIBA_BMI SINGLE 1 \| PERSIAN YYMMDD $INPUT_PARAMETER_NXDAY
./ldr_gnl_nul $USER $PASS temp_trn_shiba $TXTFILE_TRN_SHIBA_BSI SINGLE 1 \| PERSIAN YYMMDD $INPUT_PARAMETER_NXDAY

./ldr_nul $USER $PASS temp_trn_balance $TXTFILE_BLN_BALANCE SINGLE 1 \| PERSIAN YYMMDD
./ldr_nul $USER $PASS temp_trn_rate_currency $TXTFILE_BLN_CURRATE SINGLE 1 NUL PERSIAN YYMMDD

./ldr_nul $USER $PASS temp_chq_chequeqcch 	$TXTFILE_CHQ_QCCH 	SINGLE 	 1 	\| PERSIAN YYMMDD   
./ldr_nul $USER $PASS temp_chq_chequebook_n 	$TXTFILE_CHQ_BOOK 	SINGLE 	 1	\| PERSIAN YYMMDD
./ldr_nul $USER $PASS temp_chq_chequepress_n 	$TXTFILE_CHQ_PRESS 	PARALLEL 4 	\| PERSIAN YYMMDD  
./ldr_nul $USER $PASS temp_chq_chequereturn_n $TXTFILE_CHQ_RETURN SINGLE 	 1 	\| PERSIAN YYMMDD
./ldr_nul $USER $PASS std_chq_chequestop 	$TXTFILE_CHQ_STOP 	SINGLE 	 1 	\| PERSIAN YYMMDD

./ldr_nul $USER $PASS std_crd_itxn $TXTFILE_CRD_ITXN SINGLE 1 \| GREGORIAN DD/MM/YYYY   
#################################################################################
COUNT_TABLE_GNL_BRANCH=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_gnl_branch_crnt; 		
exit; 
END_OF_SQL
)
COUNT_TABLE_GNL_USER=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_gnl_user_crnt; 		
exit; 
END_OF_SQL
)
COUNT_TABLE_GNL_CUSTADDR=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_gnl_custaddr_crnt; 	
exit; 
END_OF_SQL
)
COUNT_TABLE_GNL_CUSTOMER=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_gnl_custinfo_n; 		
exit; 
END_OF_SQL
)
COUNT_TABLE_GNL_CUSTPART=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_gnl_custpart; 		
exit; 
END_OF_SQL
)
COUNT_TABLE_GNL_ACCOUNT=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_gnl_account_crnt; 	
exit; 
END_OF_SQL
)
COUNT_TABLE_GNL_CODE=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_gnl_bs_sct_crnt; 		
exit; 
END_OF_SQL
)
COUNT_TABLE_TRN_TRANSACTION=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from std_trn_transaction; 		
exit; 
END_OF_SQL
)
COUNT_TABLE_TRN_CTRINFO=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_trn_ctrinfo; 			
exit; 
END_OF_SQL
)
COUNT_TABLE_TRN_TR=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_trn_tr2; 				
exit; 
END_OF_SQL
)
COUNT_TABLE_TRN_TRNINFO=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_trn_trninfo; 		
exit;
END_OF_SQL
)
COUNT_TABLE_TRN_TRS=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_trn_trs2; 		
exit;
END_OF_SQL
)
COUNT_TABLE_TRN_SHIBA=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_trn_shiba; 		
exit;
END_OF_SQL
)
COUNT_TABLE_BLN_BALANCE=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_trn_balance; 			
exit; 
END_OF_SQL
)
COUNT_TABLE_BLN_CURRATE=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_trn_rate_currency; 
exit; 
END_OF_SQL
)
COUNT_TABLE_CHQ_QCCH=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_chq_chequeqcch; 
exit; 
END_OF_SQL
)
COUNT_TABLE_CHQ_BOOK=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_chq_chequebook_n; 
exit; 
END_OF_SQL
)
COUNT_TABLE_CHQ_PRESS=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_chq_chequepress_n; 
exit; 
END_OF_SQL
)
COUNT_TABLE_CHQ_RETURN=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from temp_chq_chequereturn_n; 
exit; 
END_OF_SQL
)
COUNT_TABLE_CHQ_STOP=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from std_chq_chequestop; 
exit; 
END_OF_SQL
)
COUNT_TABLE_CRD_ITXN=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select count(*) from std_crd_itxn; 
exit; 
END_OF_SQL
)
#################################################################################

#################################################################################
#USED ONLY FOR EXCEPTION EXIST IN CRD SYSTEM :
# --> TODAY FILES ZIPPED INTO ANOTHER DAY
# --> I DON'T KNOW WHY
# BECAUSE DOWNLOAING CARD'S FILES IS A COMPLEX OPERATION, I ADDED THIS EXTRA CONTROL TO SEE THE CORRECT FILE DOWLOADED AND THEN LOADED
V_CRD_ITXN_DATE=$(
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
set pagesize 0 feedback off verify off heading off echo off; 
select distinct to_char(capdate,'yyyymmdd','nls_calendar=persian') from std_crd_itxn;
exit; 
END_OF_SQL
)

if [ ! $V_CRD_ITXN_DATE -eq $INPUT_PARAMETER_TODAY ]; then
	printf "${COLOR_RED}File card_itxn is not valid. (select distinct to_char(capdate,'yyyymmdd','nls_calendar=persian') from std_crd_itxn)= ${COLOR_YELLOW}$V_CRD_ITXN_DATE\n${COLOR_NC}"
	exit 1
else
	printf "File card_itxn is valid. (select distinct to_char(capdate,'yyyymmdd','nls_calendar=persian') from std_crd_itxn)= ${COLOR_YELLOW}$V_CRD_ITXN_DATE\n${COLOR_NC}"
fi
#################################################################################

#################################################################################
sqlplus -s $USER/$PASS@$IP:$PORT/$SID << END_OF_SQL 
execute DLT_PKG.gnl3_fix_custinfo;
execute DLT_PKG.trn_fix_after_loading_tr;
execute DLT_PKG.trn_fix_after_loading_trs;
execute DLT_PKG.chq_fix_after_loading($INPUT_PARAMETER_TODAY);
END_OF_SQL
#################################################################################
LENGTH_TXTFILE_TRN_SHIBA_BMI_BSI=$(($LENGTH_TXTFILE_TRN_SHIBA_BMI + $LENGTH_TXTFILE_TRN_SHIBA_BSI))

checkFileLengthWithDBTable $COUNT_TABLE_GNL_BRANCH 		$LENGTH_TXTFILE_GNL_BRANCH 			GNL_BRANCH
checkFileLengthWithDBTable $COUNT_TABLE_GNL_USER 		$LENGTH_TXTFILE_GNL_USER 			GNL_USER
checkFileLengthWithDBTable $COUNT_TABLE_GNL_CUSTADDR 	$LENGTH_TXTFILE_GNL_CUSTADDR 		GNL_CUSTADDR
checkFileLengthWithDBTable $COUNT_TABLE_GNL_CUSTOMER 	$LENGTH_TXTFILE_GNL_CUSTOMER 		GNL_CUSTOMER
checkFileLengthWithDBTable $COUNT_TABLE_GNL_CUSTPART 	$LENGTH_TXTFILE_GNL_CUSTPART 		GNL_CUSTPART
checkFileLengthWithDBTable $COUNT_TABLE_GNL_ACCOUNT 	$LENGTH_TXTFILE_GNL_ACCOUNT 		GNL_ACCOUNT
checkFileLengthWithDBTable $COUNT_TABLE_GNL_CODE 		$LENGTH_TXTFILE_GNL_CODE 			GNL_CODE
checkFileLengthWithDBTable $COUNT_TABLE_TRN_TRANSACTION $LENGTH_TXTFILE_TRN_TRANSACTION 	TRN_TRANSACTION
checkFileLengthWithDBTable $COUNT_TABLE_TRN_CTRINFO 	$LENGTH_TXTFILE_TRN_CTRINFO 		TRN_CTRINFO
checkFileLengthWithDBTable $COUNT_TABLE_TRN_TR 			$LENGTH_TXTFILE_TRN_TR 				TRN_TR
checkFileLengthWithDBTable $COUNT_TABLE_TRN_TRNINFO 	$LENGTH_TXTFILE_TRN_TRNINFO 		TRN_TRNINFO
checkFileLengthWithDBTable $COUNT_TABLE_TRN_TRS 		$LENGTH_TXTFILE_TRN_TRS 			TRN_TRS
#################################################################################
#ATTENTION : 
#IN HOLIDAYS WE DON'T HAVE SUCH FILES : TRN_SHIBA_BMI , TRN_SHIBA_BSI
#SO BASED ON INPUT PARAMETRS WE DETECT TODAY IS HOLIDAY OR NOT.
#IN HOLIDAYS WE SHOULD NOT CHECK THE EXISTANCE OF THESE FILE.
if [ $INPUT_PARAMETER_TODAY = $INPUT_PARAMETER_PRDAY ]; then
	checkFileLengthWithDBTable $COUNT_TABLE_TRN_SHIBA 		$LENGTH_TXTFILE_TRN_SHIBA_BMI_BSI	TRN_SHIBA_BMI_BSI
fi
#################################################################################
checkFileLengthWithDBTable $COUNT_TABLE_BLN_BALANCE 	$LENGTH_TXTFILE_BLN_BALANCE 		BLN_BALANCE
checkFileLengthWithDBTable $COUNT_TABLE_BLN_CURRATE 	$LENGTH_TXTFILE_BLN_CURRATE 		BLN_CURRATE
checkFileLengthWithDBTable $COUNT_TABLE_CHQ_QCCH	 	$LENGTH_TXTFILE_CHQ_QCCH	 		CHQ_QCCH
checkFileLengthWithDBTable $COUNT_TABLE_CHQ_BOOK	 	$LENGTH_TXTFILE_CHQ_BOOK	 		CHQ_BOOK
checkFileLengthWithDBTable $COUNT_TABLE_CHQ_PRESS	 	$LENGTH_TXTFILE_CHQ_PRESS	 		CHQ_PRESS
checkFileLengthWithDBTable $COUNT_TABLE_CHQ_RETURN	 	$LENGTH_TXTFILE_CHQ_RETURN	 		CHQ_RETURN
checkFileLengthWithDBTable $COUNT_TABLE_CHQ_STOP	 	$LENGTH_TXTFILE_CHQ_STOP	 		CHQ_STOP
checkFileLengthWithDBTable $COUNT_TABLE_CRD_ITXN 		$LENGTH_TXTFILE_CRD_ITXN 			CRD_ITXN
#################################################################################
#ATTENTION : 
#IN HOLIDAYS WE DON'T HAVE SUCH FILES : TRN_SHIBA_BMI , TRN_SHIBA_BSI
#SO BASED ON INPUT PARAMETRS WE DETECT TODAY IS HOLIDAY OR NOT.
#IN HOLIDAYS WE SHOULD NOT CHECK THE EXISTANCE OF THESE FILE.
if [ $INPUT_PARAMETER_TODAY = $INPUT_PARAMETER_PRDAY ]; then
	if [ $SUCCESS_COUNTER == 21 ]; then
	  printf "${COLOR_YELLOW}Congratulation, ${COLOR_BLUE}all ${COLOR_YELLOW}$SUCCESS_COUNTER ${COLOR_BLUE}files loaded and fixed successfully. \n${COLOR_NC}" 
	  if [[ ! -z $4 ]] && [ $4 = 'yes' ]; then  
		executeOracleDeltaPackage $INPUT_PARAMETER_TODAY
	  fi
	else
	  printf "${COLOR_RED}Please rerun again. Total number of success is ${COLOR_YELLOW}$SUCCESS_COUNTER${COLOR_RED},Thats not enough.\n${COLOR_NC}"
	  exit 1
	fi
else
	if [ $SUCCESS_COUNTER == 20 ]; then
	  printf "${COLOR_YELLOW}Congratulation, ${COLOR_BLUE}all ${COLOR_YELLOW}$SUCCESS_COUNTER ${COLOR_BLUE}files loaded and fixed successfully. \n${COLOR_NC}" 
	  if [[ ! -z $4 ]] && [ $4 = 'yes' ]; then  
		executeOracleDeltaPackage $INPUT_PARAMETER_TODAY
	  fi
	else
	  printf "${COLOR_RED}Please rerun again. Total number of success is ${COLOR_YELLOW}$SUCCESS_COUNTER${COLOR_RED},Thats not enough.\n${COLOR_NC}"
	  exit 1
	fi
fi
#################################################################################