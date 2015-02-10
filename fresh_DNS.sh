while [ "1" = "1" ]
do
	killall -s SIGHUP dnsmasq
	sleep 5
done
