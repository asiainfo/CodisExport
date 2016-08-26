filePath=$1
echo $filePath
for var in "YSZQDJ4" "YSZQDJ3" "YSZQDJ6" "YSZQDJ5" "YSZQDJ8" "YSZQDJ7" "YSZQDJ10" "YSZQDJ9" "YSZQDJ12" "YSZQDJ11" "YSZQDJ15" "YSZQDJ13" "YSZQDJ14"
do 
ssh $var -C "source ~/.bash_profile;cd /app/codis_export/bin/;./startCodisExport.sh $filePath"
done
source ~/.bash_profile;hadoop fs -rm $filePath/_SUCCESS
flag=1
while [ 1 ]
  do
  result=`hadoop fs -ls $filePath | awk '$5==0 {print $5}'`
  for v in $result
    do
echo $v
	if [ $v=="0" ]
	   then
	   let flag=0
	fi
   done
   if [ $flag == 1 ]
	then
	echo $flag
 	break
   fi
   let flag=1
done
touch _SUCCESS
source ~/.bash_profile;hadoop fs -put _SUCCESS $filePath
