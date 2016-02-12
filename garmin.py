#!/usr/bin/python2


import csv

import logging
import logstash
from logstash_formatter import LogstashFormatter
import ConfigParser
from datetime import datetime
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('python-logstash-logger')
handler = logstash.LogstashHandler('localhost', 6400, version=1)
handlerLocal = logging.StreamHandler()
formatter = LogstashFormatter()

handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(handlerLocal)

confFile='garmin.conf'

##activity types:
#Cycling
#Lap Swimming
#Open Water Swimming
#Running
#Strength Training
#Treadmill Running


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
  elif length == 2:
    m=int(time.split(":")[0])
    s=int(time.split(":")[1])
    durationSeconds=int(m*60+s)
    durationMinutes=m
  return durationSeconds, durationMinutes

def convertNumbers(row, logDict):
  durationSeconds, durationMinutes = timeToDuration(row[csvOrderDict["Time"]])
  logDict["Duration Seconds"]=durationSeconds
  logDict["Duration Minutes"]=durationMinutes
  for field in config.get('config','csv_integers').split(','):
    try:
      if logDict[field] != "--":
        logDict[field]=int(logDict[field].strip(','))
    except:
      print field
      pass
  for field in config.get('config','csv_floats').split(','):
    try:
      if logDict[field] != "--":
        logDict[field]=float(logDict[field].strip(','))
    except:
      print field
      pass
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
    if val != "--":
      logDict[field]=val
  logDict=convertNumbers(row, logDict)
  logDict=setDate(row, logDict)
  logger.info(logDict) 

def parseCycling(row):
  logDict=dict()
  for field in config.get('activities','Cycling_fields').split(','):
    val=row[csvOrderDict[field]]
    if val != "--":
      logDict[field]=val
  logDict=convertNumbers(row, logDict)
  logDict=setDate(row, logDict)
  logger.info(logDict) 

def parseOpenWaterSwimming(row):
  logDict=dict()
  for field in config.get('activities','Open Water Swimming_fields').split(','):
    val=row[csvOrderDict[field]]
    if val != "--":
      logDict[field]=val
  logDict=convertNumbers(row, logDict)
  logDict=setDate(row, logDict)
  logger.info(logDict) 

def parseLapSwimming(row):
  logDict=dict()
  for field in config.get('activities','Lap Swimming_fields').split(','):
    val=row[csvOrderDict[field]]
    if val != "--":
      logDict[field]=val
  logDict["Distance"]=logDict["Distance"].translate(None,',m')
  logDict=convertNumbers(row, logDict)
  logDict=setDate(row, logDict)
  logger.info(logDict) 

def readCsv():
  with open('c.csv', 'r') as csvFile:
    readCsv = csv.reader(csvFile, delimiter=',')
    for row in readCsv:
      if row[csvOrderDict["Activity Type"]] == "Running" or row[csvOrderDict["Activity Type"]] == "Treadmill Running":
        parseRunning(row)
      if row[csvOrderDict["Activity Type"]] == "Cycling":
        parseCycling(row)
      if row[csvOrderDict["Activity Type"]] == "Lap Swimming":
        parseLapSwimming(row)
      if row[csvOrderDict["Activity Type"]] == "Open Water Swimming":
        parseOpenWaterSwimming(row)
      


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
