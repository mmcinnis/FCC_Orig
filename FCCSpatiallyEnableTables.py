import sys,os
import psycopg2

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
            #print str(row[0])
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
        
#
# Create spatially enabled location table
#
def ConvertLocationsToDecimalDegree():
    try:
        # Step 1
        if TableExists('lodd'):
            print 'Dropping lodd table'
            sqlQuery = "drop table lodd"
            ExecuteSQLQuery(sqlQuery)
        else:
            print 'Table lodd does not exist, continue'

        # Step 2
        print 'Creating lodd table'
        sqlQuery = "Select lo.callsign, lo.locationnumber as CallSignLocation, lo.latitudedegrees + "
        sqlQuery = sqlQuery + "cast(lo.latitudeminutes as numeric)/60 + lo.latitudeseconds/3600 as latdd, "
        sqlQuery = sqlQuery + "-1 * (lo.longitudedegrees + cast(lo.longitudeminutes as numeric)/60 + "
        sqlQuery = sqlQuery + "lo.longitudeseconds/3600) as longdd, lo.locationstate, lo.locationcity "
        sqlQuery = sqlQuery + "Into loDD "
        sqlQuery = sqlQuery + "From lo "
        sqlQuery = sqlQuery + "Order By lo.locationstate, lo.locationnumber, lo.locationcity, CallSignLocation;"

        ExecuteSQLQuery(sqlQuery)

        if TableExists('lodd'):
            # Step 1
            print 'Alter lodd table'
            sqlQuery = "Alter Table lodd Add Column the_geom geometry;"
            ExecuteSQLQuery(sqlQuery)

            # Step 2
            print 'Add Geometry to lodd table'
            sqlQuery = "Update lodd SET the_geom = ST_GeomFromText('POINT(' || longdd || ' ' || latdd || ')', 4326);"
            ExecuteSQLQuery(sqlQuery)

            # Step 3
            print 'Reproject to meters'
            sqlQuery = "SELECT callsign,callsignlocation,latdd,longdd,locationstate,locationcity,ST_Transform(the_geom,102009) As the_geom_m INTO lodd_meters FROM lodd;"
            ExecuteSQLQuery(sqlQuery)
            
    except Exception, err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        print "Exception in ConvertLocationsToDecimalDegree()"

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
# Create spatially enabled location table
#
def SpatiallyEnableTable(tabName):
    try:
        if TableExists(tabName) and not tabName == 'lc':
            # Add Geometry Column
            #sqlQuery = "Alter Table " + tabName + " Add Column the_geom geometry;"
            #print 'Add geometry field to : ' + tabName
            #ExecuteSQLQuery(sqlQuery)

            # Populate with update from lodd
            if ColumnExists(tabName, 'locationnumber'):                              
                sqlQuery = "Update " + tabName
                sqlQuery = sqlQuery + " Set the_geom = lodd_meters.the_geom_m From lodd_meters "
                sqlQuery = sqlQuery + " Where " + tabName + ".callsign = lodd_meters.callsign and "
                sqlQuery = sqlQuery + tabName + ".locationnumber = lodd_meters.callsignlocation "            
            else:
                sqlQuery = "Update " + tabName
                sqlQuery = sqlQuery + " Set the_geom = lodd_meters.the_geom_m From lodd_meters "
                sqlQuery = sqlQuery + " Where " + tabName + ".callsign = lodd_meters.callsign "
                
            print 'Updating the_geom to meters for : ' + tabName
            
            ExecuteSQLQuery(sqlQuery)
        else:
            print 'Table is "lc" or does not exist : ' + tabName

    except Exception, err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        print "Exception in SpatiallyEnableTable(tabName)"
        
#*********************************************************
# Start Working

#*********************************************************
# Validate cmd line arguments
if (len(sys.argv) == 2):        
    print 'Cmd line arguments look good'
    print
    tabName = sys.argv[1].lower()
else:
    print 'cmd line arguments missing: $ python  FCCSpatiallyEnableTables.py all'
    print
    print 'python FCCSpatiallyEnableTables.py all'
    print 'or'
    print 'python FCCSpatiallyEnableTables.py lo'
    sys.exit(0)

# SQL Query from table lo to lodd 
ConvertLocationsToDecimalDegree()

# Create points from lat/long coords
if tabName == 'all':
    tabList = GetFCCTables()
    for tab in tabList:
       SpatiallyEnableTable(tab) 
else:
    SpatiallyEnableTable(tabName)

    
print
print 'Done with FCCSpatiallyEnableTables.py'


       
