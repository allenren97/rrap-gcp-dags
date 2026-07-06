#!/bin/bash

dt=$1
data_file1=$2
errors=$3

echo "the date is $dt"
echo "the data file is $data_file1"
echo "the email type is $errors"

html_file=report.html
html_head=$html_head"MIME-Version: 1.0\n"
html_head=$html_head"Content-Type: text/html\n"
html_head=$html_head"<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\" \"http://www.w3.org/TR/html4/loose.dtd\">\n"
html_head=$html_head"<html><body>"

html_end="</body></html>"

desc="<H2 style='font-family:Calibri;'>Data Quality Check in Customer Behavior System (CBS)</H2>\n\n"
desc=$desc"<b><p style='color:black;font-family:Calibri;'>Description:</p></b>\n"
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
	data_file=$1
	table_content=""
	i=0
	isBackground=1

	rows=`wc -l $data_file |awk -F ' ' '{print $1}'`
	exec 3<>$data_file
	
	while read line
	do

		let i=$i+1

		IFS=$'\t' arr=($line)

		if [ $isBackground -eq 1 ]; then
			let isBackground=0
			table_content=$table_content"<tr style='background:#DDEBF7;border-color:black;height:20.0pt'>\n"
		else
			let isBackground=1
			table_content=$table_content"<tr style='height:20.0pt'>\n"
		fi


		let j=0

		for item in ${arr[@]}
		do
			let j=$j+1

			if [ $j -eq 8 -o $j -eq 9 ];then
				if [ $j -eq 8 ];then
                                       item=`escape $item%`
                                else
                                       item=`escape $item`
                                fi

				if [[ $errors -eq 1 && $i -eq $rows ]];then
					table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'><font color='red'>$item</font></td>\n"
				else
					table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"
				fi
			else
				item=`escape $item`
				table_content=$table_content"<td style='text-align:right;border-color:black;padding:0cm 5.4pt 0cm 5.4pt'>$item</td>\n"
			fi
		done
		table_content=$table_content"</tr>\n"

	done<&3

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
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Source Table</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Source Count</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Target Table</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Target Count</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Diff</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Message Type</font></th>\n"
	table_content=$table_content"<th width=120 style='text-align:right;padding:0cm 5.4pt 0cm 5.4pt'><font color='white'>Result Text</font></th>\n"
	table_content=$table_content"</tr>\n"

	table_content=$table_content"</font></tr></thread>\n"
}


title="Custer Behavior System(CBS) - Data Quality Check -- $dt"
sender="DoNotReply_CBS@scotiabank.com"
receiver="Cheng.liu@scotiabank.com Derrick.Phung@scotiabank.com Weili.Peng@scotiabank.com sujeetha.selvaraj@scotiabank.com amo.is@scotiabank.com EDL-applicationsupport@scotiabank.com rrmss_support@scotiabank.com GFRT-Operations@scotiabank.com"
if [[ $errors -eq 1 ]]; then
	receiver="Cheng.liu@scotiabank.com Derrick.Phung@scotiabank.com Weili.Peng@scotiabank.com sujeetha.selvaraj@scotiabank.com amo.is@scotiabank.com EDL-applicationsupport@scotiabank.com rrmss_support@scotiabank.com GFRT-Operations@scotiabank.com"
else
	receiver="Cheng.liu@scotiabank.com Derrick.Phung@scotiabank.com Weili.Peng@scotiabank.com sujeetha.selvaraj@scotiabank.com rrmss_support@scotiabank.com GFRT-Operations@scotiabank.com"
fi
#receiver="cheng.liu@scotiabank.com"
echo "[INFO] Sending email ......"

echo -e $html_head > $html_file
echo -e "$desc\n" >> $html_file

echo "processing head ..."
parse_mk_table_head
echo -e $table_content >> $html_file
echo "process contents [$data_file1]..."
parse_mk_table_content $data_file1

echo -e $table_content >> $html_file
echo -e "</table>" >> $html_file

echo -e $html_end >> $html_file


./sendemail.sh -t "$title" -f "$sender" -o "$receiver" -s "$title" -F $html_file

echo "[INFO] Finished"



