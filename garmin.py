#!/usr/bin/python2
import csv
import re
import logging
import logstash
from logstash_formatter import LogstashFormatter
import ConfigParser
from datetime import datetime
from elasticsearch import Elasticsearch
import json
import time
import optparse
import sys

#Setup Logger

def loadConfig(confFile):
  config = ConfigParser.ConfigParser()
  config.read(confFile)
  return config



def buildMatchQuery(logDict):
  '''builds the elasitcsearch query.'''
  body = {
      "filter" : {
          "bool" : {
              "must" : []
              }
          }
      }
  for field, value in logDict.items():
    if field  == "Start":
      mustQ = {
          "term" : {
              "{0}.raw".format(field) : value
              }
          }
      body["filter"]["bool"]["must"].append(mustQ)
  logger.debug(json.dumps(body, sort_keys=False, indent=2))
  return body


def queryES(body, size, fields=None):
  '''the acually query function towards elasticsearch.'''
  logger.debug("Final Query:\n{0}".format(json.dumps(body, sort_keys=False, indent=2)))
  es = Elasticsearch(["localhost"], sniff_on_start=False, sniff_on_connection_fail=False, sniffer_timeout=60)
  if fields:
    logger.debug("Fields: %s" % (fields) )
    res = es.search(index="garmin*", size=size, sort=["@timestamp:desc"], body=body, fields=fields)
  else:
    res = es.search(index="garmin*", size=size, sort=["@timestamp:desc"], body=body)
  logger.debug("Elastic Search Results\n{0}".format(json.dumps(res, sort_keys=False, indent=2)))
  return res


def checkES(logDict):
  body=buildMatchQuery(logDict)
  res = queryES(body, 1)
  return res["hits"]["total"]


def timeToDuration(time):
  length=len(time.split(":"))
  if length == 3:
    h=int(time.split(":")[0])
    m=int(time.split(":")[1])
    s=int(time.split(":")[2])
    durationSeconds=int((h*60*60)+m*60+s)
    durationMinutes=int((h*60)+m)
    return durationSeconds, durationMinutes
  elif length == 2 and time.split(":")[0] != '' :
    m=int(time.split(":")[0])
    s=int(time.split(":")[1])
    durationSeconds=int(m*60+s)
    durationMinutes=m
    return durationSeconds, durationMinutes
  return 0, 0

def convertNumbers(row, logDict):
  durationSeconds, durationMinutes = timeToDuration(row[csvOrderDict["Time"]])
  if durationSeconds >=0:
    logDict["Duration_Seconds"]=durationSeconds
  if durationMinutes >=0:
    logDict["Duration_Minutes"]=durationMinutes
  for field in config.get('config','csv_integers').split(','):
    if field in logDict:
      if "--" not in logDict[field]:
        logDict[field]=int(logDict[field].translate(None, '\",'))
      else:
        print row
  for field in config.get('config','csv_floats').split(','):
    if field in logDict:
      if "--" not in logDict[field]:
        logDict[field]=float(logDict[field].translate(None, '\",'))
      else:
        print row
  return logDict

def setDate(row, logDict):
  #format
  #Thu, Feb 11, 2016 8:43
  startDate=row[csvOrderDict["Start"]]
  timestamp=datetime.strptime(startDate, '%a, %b %d, %Y %H:%M')
  logDict["@timestamp"]=timestamp
  return logDict

def parseRunning(row):
  logDict=dict()
  for field in config.get('activities','Running_fields').split(','):
    val=row[csvOrderDict[field]]
    if "--" not in val:
      logDict[field]=val
  try:  
    durationSeconds, durationMinutes = timeToDuration(re.sub('min.*$', '', logDict["Avg Speed(Avg Pace)"]))
    logDict["Avg Pace"] = logDict["Avg Speed(Avg Pace)"]
    del logDict["Avg Speed(Avg Pace)"]
    logDict["Avg Pace_seconds"]=durationSeconds
    durationSeconds=None
  except:
    logger.debug("Missing Avg pace field:{0}".format(row))
    pass
  try: 
    durationSeconds, durationMinutes = timeToDuration(re.sub('min.*$', '', logDict["Max Speed(Best Pace)"]))
    logDict["Best Pace" ] =  logDict["Max Speed(Best Pace)"]
    del logDict["Max Speed(Best Pace)"]
    logDict["Best Pace_seconds"]=durationSeconds
  except:
    logger.debug("Missing Best pace field:{0}".format(row))
    pass 
  logDict=convertNumbers(row, logDict)
  logDict=setDate(row, logDict)
  sendEvent(logDict, row)

def parseStrengthTraining(row):
  logDict=dict()
  for field in config.get('activities','Strength Training_fields').split(','):
    val=row[csvOrderDict[field]]
    if val != "--":
      logDict[field]=val
  logDict=convertNumbers(row, logDict)
  logDict=setDate(row, logDict)
  sendEvent(logDict, row)

def parseCycling(row):
  logDict=dict()
  for field in config.get('activities','Cycling_fields').split(','):
    val=row[csvOrderDict[field]]
    if val != "--":
      logDict[field]=val
  logDict=convertNumbers(row, logDict)
  logDict=setDate(row, logDict)
  sendEvent(logDict, row)

def parseOpenWaterSwimming(row):
  logDict=dict()
  for field in config.get('activities','Open Water Swimming_fields').split(','):
    val=row[csvOrderDict[field]]
    if val != "--":
      logDict[field]=val
  logDict=convertNumbers(row, logDict)
  logDict=setDate(row, logDict)
  sendEvent(logDict, row)

def parseLapSwimming(row):
  logDict=dict()
  for field in config.get('activities','Lap Swimming_fields').split(','):
    val=row[csvOrderDict[field]]
    if val != "--":
      logDict[field]=val
    else:
      logDict[field]="0"
  try:
    logDict["Distance_Meters"]=int(logDict["Distance"].translate(None,',m'))
    logDict["Distance"]=str(float(logDict["Distance"].translate(None,',m'))/1000)
  except:
    logger.debug("Could not convert distance:{0}".format(row))
  durationSeconds, durationMinutes = timeToDuration(re.sub('min.*$', '', logDict["Avg Speed(Avg Pace)"]))
  logDict["Avg Pace"] = logDict["Avg Speed(Avg Pace)"]
  del logDict["Avg Speed(Avg Pace)"]
  logDict["Avg Pace_seconds"]=durationSeconds
  durationSeconds=None
  durationSeconds, durationMinutes = timeToDuration(re.sub('min.*$', '', logDict["Max Speed(Best Pace)"]))
  logDict["Best Pace" ] =  logDict["Max Speed(Best Pace)"]
  del logDict["Max Speed(Best Pace)"]
  logDict["Best Pace_seconds"]=durationSeconds
  logDict=convertNumbers(row, logDict)
  logDict=setDate(row, logDict)
  sendEvent(logDict, row)

def sendEvent(logDict, row):
  for key in logDict.keys():
    logDict[re.sub("\(.*$", '', re.sub(' ', '_', key))]=logDict.pop(key)
  if not OPTIONS.skipCheck:
    resCount = checkES(logDict) 
    if int(resCount) == 0:
      logger.info(logDict) 
      logger.debug("Successfully imported row:{0}".format(row))
    else:
      logger.debug("Skipping row:{0}".format(row))
  else:
    logger.info(logDict) 
    logger.debug("Successfully imported row:{0}".format(row))
    

def readCsv(csvFile):
  with open(csvFile, 'r') as csvFile:
    readCsv = csv.reader(csvFile, delimiter=',')
    for row in readCsv:
      if len(row) == 1:
        continue
      elif row[csvOrderDict["Activity Type"]] == "Running" or row[csvOrderDict["Activity Type"]] == "Treadmill Running" or row[csvOrderDict["Activity Type"]] == "Walking":
        parseRunning(row)
      elif row[csvOrderDict["Activity Type"]] == "Cycling" or row[csvOrderDict["Activity Type"]] == "Rowing" :
        parseCycling(row)
      elif row[csvOrderDict["Activity Type"]] == "Lap Swimming":
        parseLapSwimming(row)
      elif row[csvOrderDict["Activity Type"]] == "Open Water Swimming" or  row[csvOrderDict["Activity Type"]] == "Swimming":
        parseOpenWaterSwimming(row)
      elif row[csvOrderDict["Activity Type"]] == "Strength Training" or row[csvOrderDict["Activity Type"]] == "Other"  :
        parseStrengthTraining(row)
      elif row[csvOrderDict["Activity Type"]] == "Activity Type":
        continue
      else:
        logger.error("Missing parser for Activity Type:{0}".format(row[csvOrderDict["Activity Type"]]))
      

def buildActivityOrder():
  csvOrderDict=dict()
  csv_order=config.get('config', "csv_order")
  count=0
  for entry in csv_order.split(','):
    csvOrderDict[entry]=count
    count+=1
  return csvOrderDict


def parseOptions():
  '''Parse command line arguments into options used by the script.'''
  parser = optparse.OptionParser(description=__doc__)
  parser.add_option("-s", "--settings", dest="confFile",
                    help="Specify config file", metavar="FILE", default="garmin.conf")
  parser.add_option("-c", "--csv_file", dest="csvFile",
                    help="Specify csv file to use", metavar="FILE",  default="Activities.csv")
  parser.add_option("-i", "--init", dest="skipCheck",
                    help="Skip checking elasticsearch(initial load)", action="store_true", default=False)
  parser.add_option("-v", "--verbose", dest="verbose", action="count",
                  help="Increase verbosity (specify multiple times for more)")
  output_opts = optparse.OptionGroup(
        parser, 'Output options',
        'Choose none or many options as outputs',
        )
  parser.add_option_group(output_opts)
  #If no argument are given, print help
  if len(sys.argv) == 1:
      parser.print_help()
      sys.exit(1)
  (options, args) = parser.parse_args()
  return options

if __name__ == "__main__":
  OPTIONS = parseOptions()
  if OPTIONS.verbose == 1 :
    log_level = logging.DEBUG
  else:
    log_level = logging.INFO
  handler = logstash.LogstashHandler('localhost', 6400, version=1)
  logger = logging.getLogger('Garmin Log Importer')
  logger.setLevel(log_level)
  handlerLocal = logging.StreamHandler()
  handler.setLevel(logging.INFO)
  handlerLocal.setLevel(logging.INFO)
  formatter = LogstashFormatter()
  handler.setFormatter(formatter)
  logger.addHandler(handler)
  logger.addHandler(handlerLocal)

  config=loadConfig(OPTIONS.confFile)
  csvOrderDict=buildActivityOrder()
  readCsv(OPTIONS.csvFile)
