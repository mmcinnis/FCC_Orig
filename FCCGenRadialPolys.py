import sys,os
import psycopg2
import math
import datetime

#*********************************************************
# Functions
#*********************************************************
        
def TableExists(tabName):
    try:
        
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cur = conn.cursor()

        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", (tabName,))
        
        if cur.fetchone()[0]:
            return True
        else:
            return False

        cur.close()
        conn.close()

    except Exception, e:        
        print 'Exception from TableExists()'
        print
        return False

def ColumnExists(tabName,columnName):
    try:
        retVal = False
        
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cur = conn.cursor()

        if not TableExists(tabName):
            return retVal
        
        sqlQuery = "select column_name from information_schema.columns where table_name ='" + tabName +"'"
        cur.execute(sqlQuery)

        rows = cur.fetchall()
        for row in rows:
            if row[0] == columnName:
                retVal = True
                break

        return retVal

    except Exception, e:        
        print 'Exception from ColumnExists()'
        print
        return retVal

def GetFCCTables():
    try:
        tabList = []
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cur = conn.cursor()

        sqlQuery = "select table_name from information_schema.tables where char_length(table_name) = 2"
        cur.execute(sqlQuery)

        rows = cur.fetchall()
        for row in rows:
            print str(row[0])
            tabList.append(str(row[0]))

        cur.close()
        conn.close()

        return tabList
        
    except Exception, e:        
        print 'Exception from GetFCCTables()'
        print
        print e.pgerror

def DropFCCTable(tabName):
    try:
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cursor = conn.cursor()

        if TableExists(tabName):
            cursor.execute('drop table %s ' % tabName)
            conn.commit()
        
        cur.close()
        conn.close()

    except:
        print 'Exception on connection to FCC'

def ExecuteSQLQuery(sqlQuery):
    try:
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cur = conn.cursor()

        cur.execute(sqlQuery)
        
        cur.close()
        conn.commit()
        conn.close()

    except Exception, err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        print "Exception in ExecuteSQLQuery(sqlQuery)"
#
# Calculate the x,y offsets from sab vector and return new x,y
#
def GetCoords(radDirection, sab, x, y):
    try:
        retX = float(x)
        retY = float(y)
        sab = float(sab)
        radDirection = int(radDirection)

        retDict = GetRadialXY(retX, retY, radDirection, sab)        
        
        #print 'return follows : ' + str(retX)+ ',' + str(retY)
        #return {'x':retX,'y':retY}
        return retDict
    
    except Exception, err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        print "Exception in GetCoords(radDirection, sab, x, y)"

def GetRadialXY(x, y, radDirection, sab):
    try:
        #print 'dir : ' + str(radDirection)
        deg = CorrectAngle(radDirection)
        rads = (math.pi * deg)/180

        #print 'deg : ' + str(deg)
        #print 'rads : ' + str(rads)
        #print 'sab : ' + str(sab)
        #print 'old x,y : ' + str(x) + ',' + str(y)
        deltaX = CalcCosX(x, rads, sab)
        deltaY = CalcSinY(y, rads, sab)        
        #print 'new x,y : ' + str(deltaX) + ',' + str(deltaY)
        #print
        return {'x':deltaX,'y':deltaY}

    except Exception, err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        print "Exception in GetRadialXY(X, Y, radDirection))"

# Meant to correct for angle 0 degrees going straight up rather then directly right
# Cos(0) = 1 in x, our Azimuths go north with 0 so swing it around 90 degress clockwise.
# 90 degrees off and reverse direction
def CorrectAngle(inAngle):
    
    if inAngle == 0:
        outAngle = 90
    elif inAngle == 45 or inAngle == 225:
        outAngle = inAngle
    elif inAngle == 90:
        outAngle = 0
    elif inAngle == 135:
        outAngle = 315
    elif inAngle == 180:
        outAngle = 270
    elif inAngle == 270:
        outAngle = 180
    elif inAngle == 315:
        outAngle = 135
    
    return outAngle

def CalcCosX(x, angle, radius):

    return ((math.cos(angle) * radius) + x)
   
def CalcSinY(y, angle, radius):

    return ((math.sin(angle) * radius) + y)

def RadialQuery2FilePoly():
    try:
        inPath = "C:\\FCC\\"
        
        updateList = []
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cur = conn.cursor()

        sqlQuery = "Select Distinct callsign, locationnumber, antennanumber, radialdirection, distancetosab, ST_X(the_geom), ST_Y(the_geom)"
        sqlQuery = sqlQuery + " From ra_polys Where distancetosab IS NOT NULL And ST_X(the_geom) IS NOT NULL "
        sqlQuery = sqlQuery + "Order By callsign, locationnumber, antennanumber, radialdirection;"
        #Limit 80

        # Initial sql string, reset in radialDirection 315 below 
        print sqlQuery
        cur.execute(sqlQuery)
        print 'Done with query'

        OUTFILE = open(inPath + 'RadialPts.csv', 'w')
        OUTFILE.write('ID|callsign|locationnumber|antennanumber|geom \n')
        print 'Writing File : ' + inPath + 'RadialPts.csv'
        
        rowCnt = 0
        
        rows = cur.fetchall()
        geom = ''
        
        for row in rows:
            if int(row[3]) == 0:
                geom = ''                
                centerX = round(float(row[5]),2)
                centerY = round(float(row[6]),2)
                callSign = str(row[0])
                locNum = str(row[1])
                antNum = str(row[2])
                radDirection = int(row[3])
                radDistance = float(row[4]) * 1000
                firstCoord = GetRadialXY(centerX, centerY, radDirection, radDistance)

            radDirection = int(row[3])
            radDistance = float(row[4]) * 1000
            radXY = GetRadialXY(centerX, centerY, radDirection, radDistance)
            geom = geom + str(radXY['x']) + ' ' + str(radXY['y']) + ',' 
           
            if int(row[3]) == 315:
                rowCnt += 1
                geom = 'POLYGON((' + geom + str(firstCoord['x']) + ' ' + str(firstCoord['y']) + '))'
                outLine = str(rowCnt) + '|' + callSign + '|' + locNum + '|' + antNum + '|' + geom + '\n'
                print 'outLine : ' + outLine
                OUTFILE.write(outLine)
        
        cur.close()
        conn.close()
        OUTFILE.close()
        
    except Exception, e:        
        print 'Exception from RadialQuery2File('
        print

def RadialQuery2FileBuf():
    try:
        inPath = "C:\\FCC\\"
        
        updateList = []
        maxDist = 0
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cur = conn.cursor()

        sqlQuery = "Select Distinct callsign, locationnumber, antennanumber, radialdirection, distancetosab, ST_X(the_geom), ST_Y(the_geom)"
        sqlQuery = sqlQuery + " From ra_polys Where distancetosab IS NOT NULL And ST_X(the_geom) IS NOT NULL "
        sqlQuery = sqlQuery + "Order By callsign, locationnumber, antennanumber, radialdirection;"
        #Limit 80

        # Initial sql string, reset in radialDirection 315 below 
        print sqlQuery
        cur.execute(sqlQuery)
        print 'Done with query'

        OUTFILE = open(inPath + 'RadialPts.csv', 'w')
        OUTFILE.write('ID,callsign,locationnumber,antennanumber,x,y,buf \n')
        print 'Writing File : ' + inPath + 'RadialPts.csv'
        
        rowCnt = 0
        
        rows = cur.fetchall()
        for row in rows:
            if float(row[4]) > maxDist:
                centerX = float(row[5])
                centerY = float(row[6])
                maxDist = float(row[4]) * 1000
                callSign = str(row[0])
                locNum = str(row[1])
                antNum = str(row[2])
                
            #print '315'
            if int(row[3]) == 315:
                rowCnt += 1
                outLine = str(rowCnt) + ',' + callSign + ',' + locNum + ',' + antNum + ',' + str(centerX) + ',' + str(centerY) + ',' + str(maxDist) + '\n'
                OUTFILE.write(outLine)
                maxDist = 0
        
        cur.close()
        conn.close()
        OUTFILE.close()
        
    except Exception, e:        
        print 'Exception from RadialQuery2Buffer('
        print

def RadialQuery2Table():
    try:
        inPath = "C:\\FCC\\"
        
        updateList = []
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cur = conn.cursor()
        print 'exists'
        if TableExists('ra_polys'):
            DropFCCTable('ra_polys')
        print 'dropped'
        createQuery = "CREATE TABLE ra_polys (rowcnt integer,callsign varchar(7),locationnumber integer,antennanumber integer,poly_geom geometry);"
        ExecuteSQLQuery(createQuery)
        print 'created'
        sqlQuery = "Select Distinct callsign, locationnumber, antennanumber, radialdirection, distancetosab, ST_X(the_geom), ST_Y(the_geom)"
        sqlQuery = sqlQuery + " From ra Where distancetosab IS NOT NULL And ST_X(the_geom) IS NOT NULL "
        sqlQuery = sqlQuery + "Order By callsign, locationnumber, antennanumber, radialdirection;"
        #Limit 80

        # Initial sql string, reset in radialDirection 315 below 
        print sqlQuery
        cur.execute(sqlQuery)
        print 'Done with query'
        
        rowCnt = 0
        
        rows = cur.fetchall()
        geom = ''
        
        for row in rows:
            if int(row[3]) == 0:
                geom = ''                
                centerX = round(float(row[5]),2)
                centerY = round(float(row[6]),2)
                callSign = str(row[0])
                locNum = str(row[1])
                antNum = str(row[2])
                radDirection = int(row[3])
                radDistance = float(row[4]) * 1000
                firstCoord = GetRadialXY(centerX, centerY, radDirection, radDistance)

            radDirection = int(row[3])
            radDistance = float(row[4]) * 1000
            radXY = GetRadialXY(centerX, centerY, radDirection, radDistance)
            geom = geom + str(radXY['x']) + ' ' + str(radXY['y']) + ',' 
           
            if int(row[3]) == 315:
                
                rowCnt += 1
                geom = "'POLYGON((" + geom + str(firstCoord['x']) + ' ' + str(firstCoord['y']) + "))'"
                #outLine = str(rowCnt) + '|' + callSign + '|' + locNum + '|' + antNum + '|' + geom + '\n'

                sqlQuery = "INSERT INTO ra_polys (rowcnt, callsign, locationnumber, antennanumber, poly_geom) "
                sqlQuery = sqlQuery + "VALUES (" + str(rowCnt) + ",'" + callSign + "'," + locNum + "," + antNum + ",ST_GeomFromText("
                sqlQuery = sqlQuery + geom + ",102009))"

                ExecuteSQLQuery(sqlQuery)
                print 'outLine : ' + sqlQuery
                #sys.exit(0)
        
        cur.close()
        conn.close()
        
    except Exception, e:        
        print 'Exception from RadialQuery2Table('
        print
        
#
# Create spatially enabled ra_poly table
#
def GenerateRadialPolys(option):
    try:
        if option == 'buf':
            RadialQuery2FileBuf()
        elif option == 'file':
            RadialQuery2FilePoly()
        else:
            RadialQuery2Table()
                
    except Exception, err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        print "Exception in GenerateRadialPolys()"
        
#*********************************************************
# Start Working

#*********************************************************
# Validate cmd line arguments
if (len(sys.argv) >= 1):        
    print 'Cmd line arguments look good'

    # Switch between file/buffer, file/polygon, table
    if sys.argv == 2:
        option = sys.argv[1].lower()
    else:
        option = 'tab'
        
    print
else:
    print 'cmd line arguments missing: $ python  FCCGenRadialPolys.py'
    print
    print 'python FCCGenRadialPolys.py'
    print 'or'
    print 'python FCCGenRadialPolys.py buf'
    print 'or'
    print 'python FCCGenRadialPolys.py file'
    print
    print 'no arg defaults to python FCCGenRadialPolys.py tab'
    print
    sys.exit(0)

# SQL Query from table lo to lodd
startTime = datetime.datetime.now()
GenerateRadialPolys(option)
endTime = datetime.datetime.now()
    
print 'Start : ' + startTime
print 'End   : ' + endTime

print 'Done with FCCGenRadialPolys.py'


       
