#!/usr/bin/python2
import csv
import re
import logging
import logstash
from logstash_formatter import LogstashFormatter
import ConfigParser
from datetime import datetime


#Setup Logger
logger = logging.getLogger('Garmin Log Importer')
logger.setLevel(logging.INFO)
handler = logstash.LogstashHandler('localhost', 6400, version=1)
handlerLocal = logging.StreamHandler()
handlerLocal.setLevel(logging.WARN)
formatter = LogstashFormatter()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(handlerLocal)

confFile='garmin.conf'


def loadConfig(confFile):
  config = ConfigParser.ConfigParser()
  config.read(confFile)
  return config

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
  return None, None

def convertNumbers(row, logDict):
  durationSeconds, durationMinutes = timeToDuration(row[csvOrderDict["Time"]])
  if durationSeconds:
    logDict["Duration_Seconds"]=durationSeconds
  if durationMinutes:
    logDict["Duration_Minutes"]=durationMinutes
  for field in config.get('config','csv_integers').split(','):
    if field in logDict:
      if "--" not in logDict[field]:
        logDict[field]=int(logDict[field].translate(None, '\",'))
  for field in config.get('config','csv_floats').split(','):
    if field in logDict:
      if "--" not in logDict[field]:
        logDict[field]=float(logDict[field].translate(None, '\",'))
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
    logger.warn("Missing Avg pace field:{0}".format(row))
    pass
  try: 
    durationSeconds, durationMinutes = timeToDuration(re.sub('min.*$', '', logDict["Max Speed(Best Pace)"]))
    logDict["Best Pace" ] =  logDict["Max Speed(Best Pace)"]
    del logDict["Max Speed(Best Pace)"]
    logDict["Best Pace_seconds"]=durationSeconds
  except:
    logger.warn("Missing Best pace field:{0}".format(row))
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
  try:
    logDict["Distance_Meters"]=int(logDict["Distance"].translate(None,',m'))
    logDict["Distance"]=str(float(logDict["Distance"].translate(None,',m'))/1000)
  except:
    logger.warn("Could not convert swim time:{0}".format(row))
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
  logger.info(logDict) 
  logger.debug("Successfully imported row:{0}".format(row))



def readCsv():
  with open('c.csv', 'r') as csvFile:
    readCsv = csv.reader(csvFile, delimiter=',')
    for row in readCsv:
      if row[csvOrderDict["Activity Type"]] == "Running" or row[csvOrderDict["Activity Type"]] == "Treadmill Running" or row[csvOrderDict["Activity Type"]] == "Walking":
        parseRunning(row)
      elif row[csvOrderDict["Activity Type"]] == "Cycling" or row[csvOrderDict["Activity Type"]] == "Rowing" :
        parseCycling(row)
      elif row[csvOrderDict["Activity Type"]] == "Lap Swimming":
        parseLapSwimming(row)
      elif row[csvOrderDict["Activity Type"]] == "Open Water Swimming" or  row[csvOrderDict["Activity Type"]] == "Swimming":
        parseOpenWaterSwimming(row)
      elif row[csvOrderDict["Activity Type"]] == "Strength Training" or row[csvOrderDict["Activity Type"]] == "Other"  :
        parseStrengthTraining(row)
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


config=loadConfig(confFile)
csvOrderDict=buildActivityOrder()
readCsv()
