#!/usr/bin/env python
from xml.etree.ElementTree import parse,Element
import re
import argparse
from os.path import expanduser
import os.path
import shutil
import ConfigParser

def update_mongo_cfg(xmlFile, cfgs, params,brokerURL='tcp://localhost:61616?wireFormat.maxInactivityDuration=0'):
    doc = parse(xmlFile)
    root = doc.getroot()
    mc = root.find('mongo-config')
    if 'dbName' in params:
       mc.set('db',params['dbName'])
    if 'collectionName' in params:
       mc.set('collection',params['collectionName'])
    se = mc.find('servers')
    servers = se.getchildren()
    for sne in se.findall('server'):
       se.remove(sne)
    for cfg in cfgs:
       e = Element('server')
       e.set('host',cfg['host'])
       e.set('port',str(cfg['port']))
       servers.append(e)
    if brokerURL is not None:
       ace = root.find('activemq-config')
       if ace is not None:
          ace.remove(ace.find('brokerURL'))
          e = Element('brokerURL')
          e.text = brokerURL
          ace.append(e)    
    pde = root.find('pluginDir')
    if pde is not None:
       pde.text = '/var/data/foundry-es/foundry_plugins/plugins'
    lde = root.find('libDir')
    if lde is not None:
       lde.text = '/var/data/foundry-es/foundry_plugins/lib'        
    newFile = re.sub(r"\.xml$","_new.xml",xmlFile)
    doc.write(newFile)
    backupFile = xmlFile + '.bak'
    shutil.move(xmlFile,backupFile)
    shutil.move(newFile,xmlFile)
    print "wrote %s" % xmlFile


#xmlFile = '/tmp/dispatcher-cfg.xml'
#cfgs = [{'host':'burak2.crbs.ucsd.edu','port':27017}]

parser = argparse.ArgumentParser(description="update all Foundry config files")
parser.add_argument('-d','--db',metavar='mongo-db-url', required=True,
      dest='dbList', action='append', 
      help='Mongo DB hostname:[port]')
parser.add_argument('-b', dest='brokerURL',action='store',
      help='ActiveMQ broker URL')
parser.add_argument('-p', dest='configFile',action='store',
      help='config file to read the parameters from')
parser.add_argument('-n',dest='dbName', action='store',
      help='the mongo db name')
parser.add_argument('-c', dest='collectionName', action='store',
      help='the name of the record collection')

args = parser.parse_args()

#config = ConfigParser.RawConfigParser()
print(args.dbList)

cfgs = []    
for db in args.dbList:
   idx = db.find(':')
   if idx != -1:
      cfg = {}
      cfg['host'] = db[:idx]
      cfg['port'] = db[idx+1:]
      cfgs.append(cfg)
   else:
      cfg = {}
      cfg['host'] = db
      cfg['port'] = '27017' 
      cfgs.append(cfg)
params = {}
if args.dbName is not None:
   params['dbName'] = args.dbName
if args.collectionName is not None:
   params['collectionName'] = args.collectionName

spMap = {'dispatcher':'dispatcher-cfg.xml','consumers':'consumers-cfg.xml','ingestor':'ingestor-cfg.xml','man-ui':'man-ui-cfg.xml'}
home_dir = expanduser("~")
prefix = 'dev/java/Foundry-ES'
for key in spMap:
   path = os.path.join(home_dir,prefix,key, 'src/main/resources/dev/' + spMap[key])
   if os.path.exists(path):
      # print path
      update_mongo_cfg(path,cfgs,params)
   path = os.path.join(home_dir,prefix,key, 'src/main/resources/prod/' + spMap[key])
   if os.path.exists(path):
      # print path
      update_mongo_cfg(path,cfgs,params)

