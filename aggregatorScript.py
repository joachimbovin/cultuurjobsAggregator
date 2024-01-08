from caggregator import cAggregator

#create instance
c = cAggregator()

print(""""
*****************************

running ~cultuuraggregator~ for cultuurjobs.be, faro.be, halftijds.be

******************************
""" )

#createoutputfile
print("creating output file...")
currentDate = c.getDateTime()
filename = "output/output-" + currentDate + ".csv"
print(filename)

#scrape website and write to output folder 
c.writeToFile()
c.cleanOuput(filename)


#write current date to log
print("writing current date to log")
print(currentDate)
c.writeLog()


#get previous date to compare
previousDate = c.readLog()
print("comparing with file from %s" % previousDate)

#c.checkLog()

#find previous file
filenameOld = "output/output-" + previousDate[:-1] + ".csv"

#compare current and previous files to see if there are new jobs...
c.compareData(filenameOld, filename)

