#!/bin/bash
#dt=`date +%Y-%m-%d`
#last_day_last_month=`date -d "$(date +%Y-%m-01) -1 day" +%Y-%m-%d`
current_date=$1
first_day_current_month=$(date -d "$current_date" +'%Y-%m-01')
last_day_last_month=`date -d "$first_day_current_month -1 day" +'%Y-%m-%d'`
sum=0
#results=(0 0 0 0)


tables_string="'cbs_mdm_flags', 'cbs_customer_base', 'cbs_acct_base', 'cbs_cust_profile_summary'"
#tables_string="'cbs_customer_base', 'cbs_acct_base'"


echo "the date is $current_date"
echo "the email type is CBS FINAL DATA CHECK"

random_string=`head /dev/urandom | tr -dc A-Za-z0-9 | head -c 13 ; echo ''`
html_file=report_$random_string.html
html_head=$html_head"MIME-Version: 1.0\n"
html_head=$html_head"Content-Type: text/html\n"
html_head=$html_head"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n"
html_head=$html_head"<html><body>"

html_end="</body></html>"

desc="<H2 style='font-family:Calibri;'>CBS final data check in Customer Behavioral Scorecard (CBS)</H2>\n\n"
desc=$desc"<b><p style='color:black;font-family:Calibri;'>Description: check if all the required tables have been correctly populated by jobs.</p></b>\n"
desc=$desc"<p style='color:black;font-family:Calibri;'><b>Processing Date:</b> the day when the data qaulity check is invoked</p>\n"
desc=$desc"<p style='color:black;font-family:Calibri;'><b>Business Date:</b> the day when the data is ingested</p>\n"
desc=$desc"<p style='color:black;font-family:Calibri;'><b>Data Diff:</b> Ratio from source count number comparing target count number</p>\n"
desc=$desc"<p style='color:black;font-family:Calibri;'></p>\n"

title=""
table_content=""

###
## Get detail data
##

function escape()
{
	line=$1
	line=${line//&/&amp;}
	line=${line//\"/&quot;}
	line=${line//</&lt;}
	line=${line//>/&gt;}
	echo "$line"
}

function parse_mk_table_content()
{
    table_content=""
    isBackground=1
    query_result=`beeline --showHeader=false --outputformat=tsv2 -u "jdbc:hive2://sdpsvrwm0124.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0128.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0162.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0123.scglobal.ad.scotiacapital.com:2181,sdpsvrwm0127.scglobal.ad.scotiacapital.com:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "
    USE crz_cust_scorecard;
    SELECT tables.table_nm, COALESCE(jobs.tbl_count, 0) AS tbl_count
    FROM (
        SELECT job_nm AS table_nm from cbs_job_info
        WHERE job_nm in ($tables_string)
    ) AS tables
    LEFT OUTER JOIN (
        SELECT target_tbl_nm, tbl_count FROM (
            SELECT target_tbl_nm AS target_tbl_nm, count(*) AS tbl_count
            FROM risk_cbs_audit_job_log
            WHERE target_tbl_nm in ($tables_string)
            AND has_data_f='Y'
            AND eff_dt='$last_day_last_month'
            GROUP BY target_tbl_nm
        ) AS j
    ) AS jobs
    ON jobs.target_tbl_nm=tables.table_nm;
    "`
    echo "Query results: "$query_result
    query_result_string=`echo $query_result`
    IFS=' ' read -r -a query_result_array <<< "$query_result_string"
    echo "First result set: "${query_result_array[0]}" : "${query_result_array[1]}
    result_array_len=${#query_result_array[@]}
    echo "Result array length: "$result_array_len
    result_len=$((result_array_len/2))
    echo $result_len" sets of results queried from database, and they are: "$query_result

    for i in `seq 1 $result_len`
    do

        table_name_index=$(((i-1)*2))
        table_count_index=$((table_name_index+1))
        table_name=${query_result_array[table_name_index]}
        table_count=${query_result_array[table_count_index]}
        echo "Table: <"$table_name"> has "$table_count" running record(s)."
        if [ $table_count -eq 0 ]; then
            sum=1
            if [ "$isBackground" -eq "1" ]; then
                let isBackground=0
                table_content=$table_content"<tr style='background:#DDEBF7;border-color:black;height:20.0pt'>\n"
            else
                let isBackground=1
                table_content=$table_content"<tr style='height:20.0pt'>\n"
            fi

            item=`escape $current_date`
            table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"

            item=`escape $last_day_last_month`
            table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"

            item=`escape "Target Table Mission"`
            table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"

            item=`escape $table_name`
            table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"

            item=`escape $table_count`
            table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'><font color='red'>$item</td>\n"

            item=`escape ">0"`
            table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"


            table_content=$table_content"</tr>\n"

        fi

    done
	exec 3>&-
	exec 3<&-
}

#width:100%;
function parse_mk_table_head()
{

	echo -e "deal table:\t"$title
	echo -e "<Br/><b>$title</b><Br/><Br/>\n" >> $html_file
	table_content="<table border=1 style='border-collapse:collapse;border:1px solid black;border-color:black;font-family:Calibri;font-size:10.5pt;font-color:white' cellspacing='1'>\n"

	table_content=$table_content"<thread> <tr style='background:#0067A6;border-color:black;height:20.0pt'><font color='white' style="font-weight:bold" >\n"

	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Procssing Date</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Business Date</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Validation Type</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Target Table</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Table Data Ingested Count</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Target Count</font></th>\n"
	table_content=$table_content"</tr>\n"

	table_content=$table_content"</font></tr></thread>\n"
}


title="Customer Behavioral Scorecard(CBS) - CBS final data check -- $last_day_last_month"
sender="DoNotReply_CBS@scotiabank.com"
receiver="Cheng.liu@scotiabank.com sujeetha.selvaraj@scotiabank.com amo.is@scotiabank.com EDL-applicationsupport@scotiabank.com raychan@scotiabank.com rrmss_support@scotiabank.com GFRT-Operations@scotiabank.com"
#receiver="jason.hou@scotiabank.com Cheng.liu@scotiabank.com"
echo "[INFO] Sending email ......"



echo -e $html_head > $html_file
echo -e "$desc\n" >> $html_file

echo "processing head ..."
parse_mk_table_head
echo -e $table_content >> $html_file
echo "process contents [$table_data]..."
parse_mk_table_content

echo -e $table_content >> $html_file
echo -e "</table>" >> $html_file

echo -e "<p style='color:black;font-family:Calibri;'><b>Error description: </b> Please rerun the jobs listed in the table above to make sure data exists in the target table(s).</p>\n" >> $html_file

echo -e $html_end >> $html_file

if [ $sum -eq 1 ]; then
    ./sendemail.sh -t "$title" -f "$sender" -o "$receiver" -s "$title" -F $html_file
fi

rm -f $html_file
echo "[INFO] Finished"
