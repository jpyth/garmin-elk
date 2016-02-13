Scripts to parse garmin exported activities(csv), generate json logging and send to ELK. 

Requirements(pip):

*  logstash_formatter
*  python_logstash

This script is intended to use logstash UDP listener with json codec to receive logs. No additional Logstash configuration required. 

Write up [here](http://dopey.io/garmin-elk.html) about this script.

