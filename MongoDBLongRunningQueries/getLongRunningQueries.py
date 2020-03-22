#!/usr/bin/env python

"""
Filename : getLongRunningQueries.py
Description: Script to find Long Running Queries in MongoDB
Author : Ashwin Pappinisseri Puthanveedu
Date Created : 10/06/2018 (MM/DD/YYYY)
Date Modified : 10/06/2018 (MM/DD/YYYY)
Python Version: 2.7
"""

from pymongo import MongoClient, errors
import ssl
import sys
import ConfigParser
import logging
from datetime import datetime
import time
import os
from bson import json_util
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class QueryObject:
    """
    Class which holds the values of different attributes of a query
    """

    def __init__(self, opid, namespace, query, microseconds, seconds):
        self.opid = opid
        self.namespace = namespace
        self.query = query
        self.microseconds = microseconds
        self.seconds = seconds

    @property
    def getOpid(self):
        """
        :return:  the value of opid attribute
        """
        return self.opid

    def setOpid(self, opid):
        """
        Set the opid attribute
        :param opid:
        :return:
        """
        self.opid = opid

    @property
    def getNameSpace(self):
        """
        :return:  the value of namespace attribute
        """
        return self.namespace

    def setNameSpace(self, namespace):
        """
        Set the nameapce attribute
        :param namespace:
        :return:
        """
        self.namespace = namespace

    @property
    def getQuery(self):
        """
        :return:  the value of query attribute
        """
        return self.query

    def setQuery(self, query):
        """
        Set the query attribute
        :param query:
        :return:
        """
        self.query = query

    @property
    def getMicroseconds(self):
        """
        :return: The value of microseconds attribute
        """
        return self.microseconds

    def setMicroSeconds(self, microseconds):
        """
        Set the microseconds attribute
        :param microseconds:
        :return:
        """
        self.microseconds = microseconds

    @property
    def getSeconds(self):
        """
        :return:  the value of Seconds attribute
        """
        return self.seconds

    def setSeconds(self, seconds):
        """
        Set the seconds attribute
        :param seconds:
        :return:
        """
        self.seconds = seconds


class getLongRunningQueries:
    ignore_ns_list = []
    ignore_desc_list = []
    ignore_op_list = []
    ignore_query_list = []

    def mongo_connection(self, host, port, user, passwd, tenant, auth):
        """
        Creates a mongodb connecion object
        :param host: hostname of mongodb
        :param port: port in which mongodb is running
        :param user: username for mongodb authentication
        :param passwd: Password for mongodb authentication
        :param tenant: Database Name
        :param auth: Authentication is enabled or not
        :return: Mongodb Connection Object
        """
        try:
            if auth == "true":
                adb = MongoClient(host, port, ssl=True,
                                  ssl_cert_reqs=ssl.CERT_NONE)
                adb.the_database.authenticate(user, passwd,
                                              mechanism='SCRAM-SHA-1',
                                              source='admin')
            else:
                adb = MongoClient(host, port)
            db = adb[tenant]
            return db
            print("Connected successfully!!!")
        except errors.ConnectionFailure:
            print("Could not connect to MongoDB")
            sys.exit()

    def check_in_list(self, listofvalues, stringInput):
        """
        Function to check whether a particular string is there in a list of strings
        :param listofvalues: List containing strings
        :param stringInput: String to be verified
        :return: True if string is present in the list  . Else returns False
        """
        in_list = False
        for values in listofvalues:
            if values in stringInput:
                in_list = True
                break
        return in_list

    def Mail_Send(self, environment, from_addr, to_addr, smtp_host, data):
        """
        Function to send mail
        :param environment: Production Environment  to be shown in the subject
        :param from_addr: From address
        :param to_addr: To addresses seperated by comma
        :param smtp_host: SMTP Hostname
        :param data: Html Data to be included in the body
        :return:
        """
        today = datetime.utcnow()
        # now = datetime.fromtimestamp(today).strftime('%Y_%m_%d_%H_%M_%S')
        msg = MIMEMultipart('alternative')
        msg['Subject'] = "Long Running Queries in " + environment + " " + str(today) + " (GMT)"
        msg['From'] = from_addr
        msg['To'] = to_addr
        msg['header'] = "Content-Type: text/html"
        # text = "SLA Report"

        # with open("LongRunningQueryReport.html", "rb") as myfile:
        #   data = myfile.readlines()

        part1 = MIMEText(data, 'html')
        part1 = MIMEText(data.encode('utf-8'), 'html', 'utf-8')
        msg.attach(part1)
        try:
            mail = smtplib.SMTP(smtp_host)
            mail.ehlo()
            print
            '\033[1;32mEmail was sent successfully!!\033[1;m'
        except smtplib.socket.gaierror:
            print
            '\033[1;31mUnable to send email!!\033[1;m'
            sys.exit()
        mail.sendmail(from_addr, to_addr.split(','), msg.as_string())
        mail.quit()

    def getcurrentOps(self, dbconn, micro_seconds_running):
        """
        Function to get the Current Operations running
        :param dbconn: MongoDB Connection Object
        :param micro_seconds_running: Micro second value to be considered in filtering out Long Running Queries
        :return: The List of QueryObjects
        """
        runningQueriesBson = dbconn.command("currentOp")
        runningQueriesJson = json_util.loads(json_util.dumps(runningQueriesBson, default=json_util.default))
        inprogessRunningQueriesJson = runningQueriesJson["inprog"]
        #inprogessRunningQueriesJson = runningQueriesJson
        queries = []
        #print(inprogessRunningQueriesJson)
        result = ''
        for query in inprogessRunningQueriesJson:
            # print(query["ns"])
            opid = str(query["opid"])
            namespace = str(query["ns"])
            description = str(query["desc"])
            operation = str(query["op"])
            #runQuery = str(query["query"]).replace("u'", '')
            command = query["command"]
            #print(command)
            if namespace in getLongRunningQueries.ignore_ns_list:
                continue
            elif description in getLongRunningQueries.ignore_desc_list:
                continue
            elif operation in getLongRunningQueries.ignore_op_list:
                continue
            elif self.check_in_list(getLongRunningQueries.ignore_query_list, command):
                continue
            else:
                if "microsecs_running" in query:
                    query_ms = query["microsecs_running"]
                    strFilter = "Filter - "
                    strSort = " Sort - "
                    if query_ms > micro_seconds_running:
                        try:
                            strFilter = strFilter + str(command["filter"]).replace("u'", '')
                        except KeyError:
                            strFilter = "No Filter"
                        try:
                            strSort = strSort + str(command["sort"]).replace("u'", '')
                        except KeyError:
                            strSort = "No Sort"
                        runQuery = strFilter + "  "+strSort
                        # query_obj = QueryObject()
                        str_query_ms = str(query_ms)
                        # print("ns :" + namespace + "query : " + runQuery + " seconds running : " + str_query_ms)
                        # currentResult = "\n ns :" + namespace + "  query : " + runQuery + " micro seconds running : " + str_query_ms
                        # result = result + currentResult
                        seconds = long(str_query_ms) / 10000000
                        query_obj = QueryObject(opid, namespace, runQuery, str_query_ms, str(seconds))
                        queries.append(query_obj)
        return queries

    def writetoFile(self, env, foldername, typeofdb, queryList):
        """
        Function to write Long Running Queries to file
        :param env: Environment Name
        :param foldername: Output Folder name
        :param typeofdb: Type of Database (Shared/SiteSpecific)
        :param queryList: List of QueryObjects
        :return:
        """
        today = datetime.utcnow()
        with open(foldername + '/LongRunningQuery_' + env + '_' + typeofdb, 'a') as fp:
            fp.write('Long Running Query for {0}\n'.format(str(today)))
            fp.write('---------------------------------\n')
            for query in queryList:
                fp.write(
                    "\n ns :" + query.getNameSpace + "  query : " + query.getQuery + " micro seconds running : " + query.getMicroseconds)
            fp.write('\n---------------------------------\n')
            fp.close()

    def createHtmlTables(self, queryList):
        """
        Function to create html tables
        :param queryList: List of QueryObjects
        :return: Returns HTML table string
        """
        htmlString = '<table border = "1" cellpadding = "2" cellspacing = "1"><tr><th>Namespace</th><th>Queries</th><th>Seconds</th><th>Operation ID</th></tr>'
        for query in queryList:
            htmlString = htmlString + "<tr><td>" + query.getNameSpace + "</td><td>" + query.getQuery + "</td><td>" + query.getSeconds + "</td><td>" + query.getOpid + "</td></tr>"
        htmlString = htmlString + "</table>"

        return htmlString

    def createReport(self, sharedHtmlTable, priHtmlTable, drHtmlTable):
        """
        Function to create Html Report
        :param sharedHtmlTable: Shared Html tables
        :param priHtmlTable: Primary SiteSpecific HTML Table
        :param drHtmlTable: DR SiteSpecific HTML Table
        :return: Return complete HTML report as String
        """
        htmlString = "<html><body><h1>Shared MongoDB</h1>" + sharedHtmlTable + "\n\n<h1>PrimarySite Specific MongoDB</h1>" + priHtmlTable + "\n\n<h1>DRSite Specific MongoDB</h1>" + drHtmlTable + "\n\n</body></html>"
        with open("LongRunningQueryReport.html", 'w') as fp:
            fp.write(htmlString)
            fp.close()
        return htmlString


def main():
    """ Configuration  Parser """
    config = ConfigParser.ConfigParser()
    config.read("config.ini")
    """
    Reads configurations from config.ini file
    """
    environment = config.get("main", "environment")
    client = config.get("main", "client")
    outputFolder = config.get("main", "outputfolder")
    host = config.get('mongo', 'host')
    port = config.get('mongo', 'port')
    prsitehost = config.get('mongo', 'primarysitespecifichost')
    prsiteport = config.get('mongo', 'primarysitespecificport')
    drsitehost = config.get('mongo', 'drsitespecifichost')
    drsiteport = config.get('mongo', 'drsitespecificport')
    user = config.get('mongo', 'user')
    passwd = config.get('mongo', 'passwd')
    # start = config.get('mongo', 'start')
    # end = config.get('mongo', 'end')
    auth = config.get('mongo', 'auth')
    micro_seconds_running = float(config.get('mongo', 'microseconds'))
    ignore_ns = config.get('mongo', 'ignorens')
    getLongRunningQueries.ignore_ns_list = ignore_ns.split(',')
    ignore_desc = config.get('mongo', 'ignoredesc')
    getLongRunningQueries.ignore_desc_list = ignore_desc.split(',')
    ignore_op = config.get('mongo', 'ignoreop')
    getLongRunningQueries.ignore_op_list = ignore_op.split(',')
    ignore_query = config.get('mongo', 'ignorequery')
    getLongRunningQueries.ignore_query_list = ignore_query.split(',')
    loglevel = config.get('logger', 'loglevel')

    #me = "no-reply@actiance.com"  # Setting the from address
    mail_id = config.get("main", "Mail_id")
    me =  config.get("main", "Mail_id_from")
    smtp_server = config.get("main", "smtp_server")

    """
    Creating logger configurations
    """

    try:
        os.mkdir("logs")
    except OSError:
        print("logs directory already exists")
    ts = time.time()
    now = datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H_%M_%S_%f')
    logger = logging.getLogger("GET DATA FROM SUPERVISED_ITEMS")
    file_log_handler = logging.FileHandler("logs/getLongRunningQueries" + ".log")
    logger.addHandler(file_log_handler)
    strerr_log_handler = logging.StreamHandler()
    logger.addHandler(strerr_log_handler)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    file_log_handler.setFormatter(formatter)
    strerr_log_handler.setFormatter(formatter)
    logger.setLevel(loglevel)
    exp = getLongRunningQueries()
    """ Connecting to Mongo """
    dbconn = exp.mongo_connection(host, int(port), user, passwd, 'admin',
                                  auth)  # Creating shared mongodb connection objects
    prsiteDBConn = exp.mongo_connection(prsitehost, int(prsiteport), user, passwd, 'admin',
                                        auth)  # Creating primary sitespecific mongodb connection objects
    drsiteDBConn = exp.mongo_connection(drsitehost, int(drsiteport), user, passwd, 'admin',
                                        auth)  # Creating DR sitespecific mongodb connection objects
    logger.info("Connected to Mongo")

    sharedQuery = exp.getcurrentOps(dbconn, micro_seconds_running)  # Get Long Running Queries from Shared MongoDB
    prisiteQuery = exp.getcurrentOps(prsiteDBConn,
                                     micro_seconds_running)  # Get Long Running Queries from Primary Sitespecific  MongoDB
    drsiteQuery = exp.getcurrentOps(drsiteDBConn,
                                    micro_seconds_running)  # Get Long Running Queries from DR Sitespecific MongoDB

    htmlReportRequired = False
    sharedHtml = 'No Long Running Queries'
    primaryHtml = 'No Long Running Queries'
    drHtml = 'No Long Running Queries'

    if len(sharedQuery) > 0:
        htmlReportRequired = True
        exp.writetoFile(environment, outputFolder, 'SharedDB', sharedQuery)
        sharedHtml = exp.createHtmlTables(sharedQuery)
    if len(prisiteQuery) > 0:
        htmlReportRequired = True
        exp.writetoFile(environment, outputFolder, 'SiteOne', prisiteQuery)
        primaryHtml = exp.createHtmlTables(prisiteQuery)
    if len(drsiteQuery) > 0:
        htmlReportRequired = True
        exp.writetoFile(environment, outputFolder, 'SiteTwo', drsiteQuery)
        drHtml = exp.createHtmlTables(drsiteQuery)

    """
    Send Mail only if there are some long running queries
    """
    if htmlReportRequired:
        htmlData = exp.createReport(sharedHtml, primaryHtml, drHtml)
        exp.Mail_Send(client+" "+environment, me, mail_id, smtp_server, htmlData)


if __name__ == "__main__":
    main()
