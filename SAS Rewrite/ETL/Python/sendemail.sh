#!/usr/bin/bash

if [ $# -gt 0 ]; then
    while [ "$1" != "" ]; do
        case $1 in
            -t | --title )  shift
                        title=$1;;
            -f | --from )  shift
                        from=$1;;
            -o | --to )  shift
                        to=$1;;
            -c | --cc )  shift
                        cc=$1;;
            -b | --bcc )  shift
                        bcc=$1;;
            -s | --subject )  shift
                        subject=$1;;
            -F | --file )  shift
                        file=$1;;
        *) usage;;
        esac
        shift
    done
fi
echo "title=$title, from=$from, to=$to, cc=$cc, bcc=$bcc, subject=$subject, file=$file"

file_name_random=`head /dev/urandom | tr -dc A-Za-z0-9 | head -c 13 ; echo ''`
temp_file_name=temp_email_tosent_$file_name_random

(
echo "From: $from"
echo "To: $to"
echo "Cc: $cc"
echo "Bcc: $bcc"
echo "Subject: $subject"
cat $file
) > $temp_file_name
cat $temp_file_name | sendmail -t
rm -f $temp_file_name


