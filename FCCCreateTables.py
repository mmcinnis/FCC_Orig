import sys,os
import psycopg2

#*********************************************************
# Creates a new FCC database from *.dat files
#*********************************************************
# http://wireless.fcc.gov/uls/index.htm?job=transaction&page=weekly
# unzip Cellular/Licenses and save data dictionary as a text file
# http://wireless.fcc.gov/uls/data/documentation/pa_ddef41.pdf
# all in C:\FCC

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
                if row[0].startswith('geo') or row[0].startswith('raster') or row[0].startswith('spatial') :
                    print 'PostGIS requirement : Skipping : '  + str(row[0])
                else:
                    cursor.execute('drop table %s cascade ' % row[0])
                    print "dropping %s" % row[0]
            except:
                print "couldn't drop %s" % row[0]
    
        conn.commit()
        conn.close()

    except:
        print 'Exception on connection to FCC'
        
#
# Check for an expected field in a Feature Class or Table
#
def CreateFCCTable(datFile, defFilePath):  
    INFILE = open(defFilePath, 'r')
    NoLineMatch = True
    postGresCmd = ''
    reservedCount = 1
    
    try:
        conn = psycopg2.connect("dbname='FCC' user='postgres' host='localhost' password='yamanami'")
        cur = conn.cursor()
        
        # Read lines until the datFile line is detected [RA]
        while NoLineMatch:
            line = INFILE.readline()

            if line.find(datFile) != -1:
                NoLineMatch = False
                print 'Got a match with : ' + datFile
                tabName = datFile.replace('[','')
                tabName = tabName.replace(']','')
                postGresCmd = 'CREATE TABLE ' + tabName + ' ('         

        # Read the datFile fields until an empty line is detected
        while len(line) > 2:

            # Deal with multiple fields name 'Reserved'
            if line.find('Reserved') != -1:
                line = line.replace('Reserved', 'Reserved' + str(reservedCount))
                reservedCount += 1

            tokCount = line.split(' ')

            # Concat each field and data type
            if len(tokCount) > 2:
                postGresCmd = postGresCmd + ParseFieldAndType(line)

            # Get the next field
            line = INFILE.readline()

        # Tack on the cmd ending and deal with two field mismatches
        print 'tabName : ' + tabName
        if tabName == 'MK':
            postGresCmd = postGresCmd + ',phantom1 text, phantom2 text)'
        elif tabName == 'L3':
            postGresCmd = postGresCmd + ',phantom1 text, phantom2 text, phantom3 text)'
        else:
            postGresCmd = postGresCmd + ')'

        # Replace initial comma
        postGresCmd = postGresCmd.replace('(,', '(')
        
        print postGresCmd
        cur.execute(postGresCmd)
        
        print 'just executed command'

        conn.commit()
        conn.close()
    
        INFILE.close()

    except Exception, e:
        print "Exception creating table : " + tabName
        print
        print e.pgerror
        
#*********************************************************
# Isolate field name and variable type from data dictionary file
def ParseFieldAndType(line):
    startsWithDigit = False
    fieldType = ''
    fieldName = ''
    
    tokArray = line.split(' ')
    tokCount = len(tokArray)
                             
    #if starts with num
    if tokArray[0].isdigit():
        print 'starts with digit'
        startsWithDigit = True

    # switch on datatype
    print 'Last element : ' + str(tokArray[tokCount - 2])

    if tokArray[tokCount - 2].endswith('yyyy'):
        print 'date'
        fieldType = 'text'
    elif tokArray[tokCount - 2].find('char') != -1:
        print 'char'
        fieldType = 'varchar(100)'
    elif tokArray[tokCount - 2].find('numeric') != -1:
        print 'numeric'
        fieldType = 'numeric'
    elif tokArray[tokCount - 2].find('int') != -1:
        print 'int'
        fieldType = 'integer'
    elif tokArray[tokCount - 2].endswith('Used') or tokArray[tokCount - 2].endswith('money'):
        print 'money/numeric'
        fieldType = 'real'
    else:
        print 'no match'
        fieldType = 'text'

    if startsWithDigit:
        for i in range(1,len(tokArray) - 2):
            fieldName = fieldName + tokArray[i]
    else:
        for i in range(0,len(tokArray) - 2):
            fieldName = fieldName + tokArray[i]

    if fieldName.endswith(']'):
        truncName = fieldName.split('[')
        fieldName = truncName[0]

    fieldName = FixFieldName(fieldName)

    # Super long text exceptions
    if fieldName.lower().find('description') != -1 or fieldName.lower().find('freeform') != -1 :
        fieldType = 'varchar(1000)'
    
    return ',' + fieldName + ' ' + fieldType

#*********************************************************
# Deal with illegal characters in the field names
def FixFieldName(inFieldName):
        
    if inFieldName == 'Fixed':
        inFieldName = 'FixedField'
        
    if inFieldName.find('-') != -1:
        inFieldName = inFieldName.replace('-', '')

    if inFieldName.find(' ') != -1:
        inFieldName = inFieldName.replace(' ', '')
        
    if inFieldName.find('(') != -1:
        outFieldName = ''
        
        for c in inFieldName:            
            if c == '(':
                break
            else:
                outFieldName = outFieldName + c

        inFieldName = outFieldName

    if inFieldName.find('mm/dd/yyyy') != -1:
        inFieldName = inFieldName.replace('mm/dd/yyyy', '')

    if inFieldName.find('/') != -1:
        inFieldName = inFieldName.replace('/', '')
        
    return inFieldName

#*********************************************************
# Validate cmd line arguments
if (len(sys.argv) == 3):
    inPath = sys.argv[1]
    inSuf = sys.argv[2]

    if os.path.exists(inPath):
        print 'In path exists : ' + inPath
        if not os.path.exists(inPath + 'pa_ddef41.txt'):
            print 'missing field definition file : ' + 'pa_ddef41.txt'
            sys.exit(2)
    else:
        print 'Missing cmd line path : ' + inPath
        sys.exit(1)
        
    print 'Cmd line arguments look good'
else:
   print 'cmd line arguments missing: $ python  FCCCreateTables.py  inPath fileSuffix'
   print
   print 'python FCCCreateTables.py C:\FCC\ .dat'
   sys.exit(0)

#*********************************************************
# Start Working

DropFCCTables()

for files in os.listdir(inPath):
    if files.find(inSuf) != -1:
        print files
        preFix = files.split('.')
        
        CreateFCCTable('[' + preFix[0].upper() + ']', inPath + 'pa_ddef41.txt')
        
print
print 'Done with FCCCreateTables.py'


       
