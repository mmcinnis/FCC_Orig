import sys,os
import psycopg2

#*********************************************************
# Functions
#*********************************************************

def DropFCCTables():
    try:
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cursor = conn.cursor()
        
        cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name""")
        rows = cursor.fetchall()
        for row in rows:
            try:
                cursor.execute('drop table %s cascade ' % row[0])
                print "dropping %s" % row[0]
            except:
                print "couldn't drop %s" % row[0]
    
        conn.commit()
        conn.close()

    except:
        print 'Exception on connection to FCC'
    
#
# Load file into table
#
def LoadFCCTable(inPath, datFile):
    try:
        notEOF = True
        INFILE = open(inPath + datFile, 'r')
        print
        print 'file just opened : ' + datFile
        line = INFILE.readline()
        
        csvArray = line.split('|')
        fileColumnCnt = len(csvArray)
        
        tabArray = datFile.split('.')
        tabName = tabArray[0].lower()

        INFILE.close()
        INFILE = open(inPath + datFile, 'r')
        
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cur = conn.cursor()

        sqlQuery = "select * from " + tabName

        cur.execute(sqlQuery)
        tableColumnCnt = len(cur.description)

        if fileColumnCnt != tableColumnCnt:
            print 'MISMATCH!!!!!!! in column count!'
            print line
            print 'file count : ' + str(fileColumnCnt)
            print 'table count : ' + str(tableColumnCnt)
            return
        else:
            print 'column counts match for : ' + datFile

        # Bulk Copy equivalent
        cur.copy_from(INFILE, tabName, '|', '')
        
        cur.close()
        conn.commit()
        conn.close()
        
        INFILE.close()

    except Exception, err:
        sys.stderr.write('ERROR: %s\n' % str(err))
        print "Exception loading table : " + datFile

#*********************************************************
# Start Working

#*********************************************************
# Validate cmd line arguments
if (len(sys.argv) == 3):
    inPath = sys.argv[1]
    inSuf = sys.argv[2]

    if os.path.exists(inPath):
        print 'In path exists : ' + inPath
    else:
        print 'Missing cmd line path : ' + inPath
        sys.exit(1)
        
    print 'Cmd line arguments look good'
else:
   print 'cmd line arguments missing: $ python  FCCLoadTables.py  inPath fileSuffix'
   print
   print 'python FCCLoadTables.py C:\FCC\ .dat'
   sys.exit(0)
   
for files in os.listdir(inPath):
    if files.find(inSuf) != -1:
        preFix = files.split('.')
        
        LoadFCCTable(inPath, files)
        
print
print 'Done with FCCFromFiles.py'


       
