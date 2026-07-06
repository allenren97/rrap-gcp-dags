#!/bin/bash
#******************************************************************
# Author: Justin Liu
# Date: Septermber 18, 2017
# Version 1.0
# Description: Generate the CSV file with fixed length of column
#   The 18th day the data is monthly data, need change the file name
#   The 18th day should use KQ calendar to calculate
#
#

source set_env.sh

DBNAME="crz_cust_scorecard"
TABLENAME="cbs_model_scorecrd_probe_fxd_len_output"
FILEPATH="/landing1/outgoing/bbcx/8m"
#FILEPATH="./8m"
WORKFOLDER="archive"

function OUTPUT_FILE (){

	#get data from source table
	hive_sql="""
		select concat(recrd_type,
			proc_dt,
			period_ind,
			cust_cid,
			seg_num,
			score,
			buffer)
		from $DBNAME.${TABLENAME}
		where eff_dt = '${BusinessDate}'
			and date_type = '$RunTypeF'
	"""
	echo $hive_sql
	beeline --showHeader=false --outputformat=tsv2 -u "${beeline_cmd}" -e "$hive_sql" > $temp_result
	rc=$?; if [[ $rc != 0 ]]; then exit $rc; fi
	job_info=`head -1 $temp_result`
	if [ "x$job_info" = "x" -o "x$job_info" = "xNULL" ]; then
		echo "Could not find any data for date: $BusinessDate"
		touch $temp_result
		#exit 1
	fi
	echo $job_info

}


function SAVE_FILE () {
	if [ $RunType = "D" ]; then
		filename="edl_out_cbs_cust_score_d_${ProcessingDateFormatted}.csv"
	fi
	if [ $RunType = "M" ]; then
		filename="edl_out_cbs_cust_score_m_${ProcessingDateFormatted}.csv"
	fi	
	source_filename=../${WORKFOLDER}/$filename
	target_filename=$FILEPATH/$filename
	
	time_id=`date +%d%b%y:%H:%M:%S`
	echo "$time_id: start to save to edge node for $filename"

	head_info=`GENERATE_HEADER`
	foot_info=`GENERATE_FOOTER`
	echo $head_info
	echo $foot_info
	echo $source_filename
	echo $target_filename

	echo $head_info > $source_filename
	cat $temp_result >> $source_filename
	echo $foot_info >> $source_filename
	
	cp $source_filename $target_filename
}

function GENERATE_HEADER (){
	record_type="1"
	file_id="EDLMDS${RunType}"
	header_date=$ProcessingDateFormatted
	head_line=$record_type$file_id$header_date
	echo $head_line
}

function GENERATE_FOOTER (){
	record_type="9"
	record_count=`wc -l $temp_result|awk '{print $1}'`
	record_count=$(printf %09d $record_count)
	foot_line=$record_type$record_count
	echo $foot_line	
	
}

function CLEAN_FILE (){
	#clean up the last 12 month's data
	last_12_month_day=`date -d "360 days ago ${ProcessingDate:0:7}-01" +%Y%m%d`
	last_12_month_day=${last_12_month_day:0:6}
	if [ "x$last_12_month_day" = "x" -o "x$last_12_month_day" = "xNULL" ]; then
		exit 0
	fi
	echo "edl_out_cbs_cust_score*${last_12_month_day}*.csv"
	find ../$WORKFOLDER/ -name "edl_out_cbs_cust_score*${last_12_month_day}*.csv" -delete
	find $FILEPATH/ -name "edl_out_cbs_cust_score*${last_12_month_day}*.csv" -delete
}

if [ $# -eq 2 ]; then
	ProcessingDate=$1
	BusinessDate=`date -d "1 days ago ${ProcessingDate:0:7}-01" +%Y-%m-%d`
	RunTypeF=$2
	RunType=${RunTypeF:0:1}
else
	echo "
	Usage: 
		./edl_out_cbs_cust_score.sh process_date date_type
	Example:
		load_data_with_loop.sh 2019-01-10 Monthly"
	exit 1

fi

ProcessingDateFormatted=`date -d "${ProcessingDate}" +%Y%m%d`
temp_result="temp_result_$TABLENAME.txt"

start_time=`date +%d%b%y:%H:%M:%S`
echo "$start_time: start to get Proble outgoing data"

OUTPUT_FILE
 
SAVE_FILE

CLEAN_FILE

end_time=`date +%d%b%y:%H:%M:%S`
echo "$end_time: end to get Proble outgoing data"

