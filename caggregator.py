# -*- coding: utf-8 -*-

import pandas as pd
from bs4 import BeautifulSoup
import requests
import datetime
import os
import time


#testgit1

class cAggregator(object):
    def __init__(self):
        self.test = None


    def getDateTime(self):
            datenow = datetime.datetime.now()
            mydate = '%s%s%s' % (datenow.year, datenow.month, datenow.day)
            return mydate


    def cleanOuput(self, outputCSV):
        """cleans the output and writes an new file"""
        name = outputCSV
        dataframe = pd.read_csv(outputCSV)
        #remove "/t/t/t nieuw/n" from column werkgever
        dataframe["werkgever"] = [item[:-10] if "Nieuw" in item else item for item in dataframe["werkgever"]]
        try:
            dataframe = dataframe.drop(['Unnamed: 0'], axis=1)
        except KeyError:
            pass
        dataframe.to_csv(name[:-4] + '-clean.csv', index=False)


    def readOutput(self, outputCSV):
        df = pd.read_csv(outputCSV)
        return df


    def requestCultuurjobs(self):
            pagelink ='http://www.cultuurjobs.be/'
            pageresponse = requests.get(pagelink, timeout=10)
            pagecontent = BeautifulSoup(pageresponse.content, "html.parser")
            return pagecontent


    def requestFaro(self):
        pagelink ='https://faro.be/vacatures'
        pageresponse = requests.get(pagelink, timeout=5)
        pagecontent = BeautifulSoup(pageresponse.content, "html.parser")
        return pagecontent

    def getCultuurjobs(self, pagecontentcultuurjobs):
        cultuurjobs = {"site": [], "job": [], "link": [], "locatie": [], "statuut": [], "deadline": [], "werkgever": []}

        lists = pagecontentcultuurjobs.findAll("li", {"class": "item"})

        for i in range(len(lists)):
            job = lists[i].find("span", {"class": "cultuurjob"})
            link = lists[i].find("a")
            metadata = link.find("span", {"class": "metadata"})
            metadatasplit = metadata.text.split("-")
            werkgever = link.find("span", {"class": "company"})
            cultuurjobs["site"].append("cultuurjobs.be")
            cultuurjobs["job"].append(job.text)
            cultuurjobs["link"].append(link["href"])
            try:
                cultuurjobs["locatie"].append(metadatasplit[0].strip())
            except IndexError:
                cultuurjobs["locatie"].append(metadata.text)
            try:
                cultuurjobs["statuut"].append(metadatasplit[1].strip())
            except IndexError:
                cultuurjobs["statuut"].append(metadata.text)
            try:
                cultuurjobs["deadline"].append(metadatasplit[2].strip())
            except IndexError:
                cultuurjobs["deadline"].append(metadata.text)
            cultuurjobs["werkgever"].append(werkgever.text)
        return cultuurjobs



    def getFaro(self, pagecontentfaro):
        faro = {"site": [], "job": [], "deadline": [], "werkgever": []}
        table = pagecontentfaro.findAll("tr")
        for i in range(len(table)):
            if i == 0:
                pass
            else:
                tableRow = table[i].findAll("td")
                #print(tableRow)
                faro["site"].append("faro.be")
                faro["job"].append(tableRow[0].text.strip())
                faro["deadline"].append(tableRow[1].text.strip())
                faro["werkgever"].append(tableRow[2].text.strip())
        return faro


    def writeToFile(self):
        """scrapes the webpages and write to a file entitled output-YYYYYMMDD.csv"""
        mydate = self.getDateTime()
        pageContentCultuurjobs = self.requestCultuurjobs()
        cultuurjobs = self.getCultuurjobs(pageContentCultuurjobs)
        dfCultuurjobs = pd.DataFrame.from_dict(cultuurjobs)
        pageContentFaro = self.requestFaro()
        faro = self.getFaro(pageContentFaro)
        dfFaro = pd.DataFrame.from_dict(faro)
        df3 = pd.merge(dfCultuurjobs, dfFaro, how='outer')
        df3.to_csv('output-' + mydate + '.csv')


    #def fixEncoding(self):
        #correct_unicode_string = test.encode('latin1').decode('utf8')
        #dataframe["job"] = [item ]


    def compareDate(self):
        mydate = self.getDateTime()
        newdate1 = time.strptime(mydate, "%Y%m%d")
        newdate2 = time.strptime("20231023", "%Y%m%d")
        print(newdate1)
        print(newdate2)
        print(newdate2 < newdate1)


    def getData(self):
        pageContentCultuurjobs = self.requestCultuurjobs()
        cultuurjobs = self.getCultuurjobs(pageContentCultuurjobs)
        dfCultuurjobs = pd.DataFrame.from_dict(cultuurjobs)
        pageContentFaro = self.requestFaro()
        faro = self.getFaro(pageContentFaro)
        dfFaro = pd.DataFrame.from_dict(faro)
        df3 = pd.merge(dfCultuurjobs, dfFaro, how='outer')
        return df3

    def compareData(self, output1, output2):
        """
        compares the previous and the most recent csv and writes a csv with the new jobs
        :param old csv:
        :param new csv:
        :return: /
        """
        oldDf = pd.read_csv(output1, index_col=None)
        newDf = pd.read_csv(output2, index_col=None)
        mydata = oldDf.merge(newDf, indicator = True, how = 'outer')
        mydata = mydata.loc[lambda x : x['_merge'] != 'both']
        mydata = mydata.loc[lambda x : x['_merge'] != 'left_only']
        mydata.to_csv("new-jobs.csv")




    def readOld(self):
        """checks the number of csv files in the directory"""
        mydate = self.getDateTime()
        path = os.getcwd()
        dir_list = os.listdir(path)
        print(dir_list)
        outputs = []
        for item in dir_list:
            if item[:6] == "output":
                outputs.append(item)
        print(outputs)
        print(len(outputs))
        if len(outputs) > 1:
            print("ERROR: there are too many output.csv files in the directory, please only use the latest file")
        #dfNew = pd.read_csv("output" + mydate + ".csv")
        dfOld = pd.read_csv(outputs[0])
        return dfOld


if __name__ == "__main__":


    print("testing")
    c = cAggregator()
    #c.cleanOuput("output-20231113.csv")
    #c.compareData("output-20231110-clean.csv", "output-20231113-clean.csv")
    #c.getDateTime()
    #c.writeToFile()
    #c.allMethods()
    #c.readCompare()
    #c.compareDate()
    #c.compareData()



#if __name__ == "__main__":


