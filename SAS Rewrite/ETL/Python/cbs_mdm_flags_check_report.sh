#!/bin/bash
dt=$1
last_day_last_month=$2
error_count=$3
table_data='cbs_mdm_flags'

echo "the date is $dt"
echo "the data file is $table_data"
echo "the email type is Missing CBS_MDM_FLAGS"

random_string=`head /dev/urandom | tr -dc A-Za-z0-9 | head -c 13 ; echo ''`
html_file=report_$random_string.html
html_head=$html_head"MIME-Version: 1.0\n"
html_head=$html_head"Content-Type: text/html\n"
html_head=$html_head"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n"
html_head=$html_head"<html><body>"

html_end="</body></html>"

desc="<H2 style='font-family:Calibri;'>CBS_MDM_FLAG data check in Customer Behavioral Scorecard (CBS)</H2>\n\n"
desc=$desc"<b><p style='color:black;font-family:Calibri;'>Description: Check if table <cbs_mdm_flags> correctly populated by jobs.</p></b>\n"
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

    if [ $isBackground -eq 1 ]
    then
        let isBackground=0
        table_content=$table_content"<tr style='background:#DDEBF7;border-color:black;height:20.0pt'>\n"
    else
        let isBackground=1
        table_content=$table_content"<tr style='height:20.0pt'>\n"
    fi

    item=`escape $dt`
    table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"

    item=`escape $last_day_last_month`
    table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"

    item=`escape Count`
    table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"

    item=`escape $table_data`
    table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"

    item=`escape $1`
    table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'><font color='red'>$item</td>\n"

    item=`escape ">0"`
    table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"


    table_content=$table_content"</tr>\n"


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


title="Customer Behavioral Scorecard(CBS) - CBS_MDM_FLAGS check -- $last_day_last_month"
sender="DoNotReply_CBS@scotiabank.com"
receiver="Cheng.liu@scotiabank.com sujeetha.selvaraj@scotiabank.com amo.is@scotiabank.com EDL-applicationsupport@scotiabank.com rrmss_support@scotiabank.com GFRT-Operations@scotiabank.com"
#receiver="jason.hou@scotiabank.com Cheng.liu@scotiabank.com"
echo "[INFO] Sending email ......"

echo -e $html_head > $html_file
echo -e "$desc\n" >> $html_file

echo "processing head ..."
parse_mk_table_head
echo -e $table_content >> $html_file
echo "process contents [$table_data]..."
parse_mk_table_content $error_count

echo -e $table_content >> $html_file
echo -e "</table>" >> $html_file

echo -e "<p style='color:black;font-family:Calibri;'><b>Error description: </b> Please rerun the job 'cbs_mdm_flags' to make sure the data exists in the table 'cbs_mdm_flags'.</p>\n" >> $html_file

echo -e $html_end >> $html_file


./sendemail.sh -t "$title" -f "$sender" -o "$receiver" -s "$title" -F $html_file

rm -f $html_file
echo "[INFO] Finished"
