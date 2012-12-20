import sys,os

# Check for expected field in a Feature Class or Table
#
def FixFCCFile(inPath, datFile):
    try:
        print 'Start FixFCCFile'

        checked = False
        INFILE = open(inPath + datFile, 'r')
        OUTFILE = open(inPath + datFile + '.out', 'w')
        
        print
        print 'File just opened : ' + datFile        

        # Strip out all cr's, makes it a one line file (*.out)
        line = 'start'
        while line:

            line = INFILE.readline()

            if not checked:
                # Record the field count
                csvArray = line.split('|')        
                fileColumnCnt = len(csvArray)
                checked = True
                        
            finLine = line.strip('\r\n')

            OUTFILE.write(finLine)
            
        INFILE.close()
        OUTFILE.close()

        # Re-introduce cr's with proper field count (*.dat)
        print 'infile : ' + inPath + datFile + '.out'
        INFILE = open(inPath + datFile + '.out', 'r')
        OUTFILE = open(inPath + datFile, 'w')
        print 'outfile : ' + inPath + datFile
        line = INFILE.readline()
        tokinize = line.split('|')

        outLine = ''
        i = 0
        
        for tok in tokinize:
            i += 1
            #print 'fileColmnCnt : ' + str(fileColumnCnt - 1)
            if i == fileColumnCnt - 1:
                outLine = outLine + tok + '|\n'
                OUTFILE.write(outLine)
                outLine = ''
                i = 0
            else:
                outLine = outLine + tok + '|'
            
    except:
        print "Exception with file : " + datFile

#*********************************************************
# Start Working

#*********************************************************
# Validate cmd line arguments
if (len(sys.argv) == 3):
    inPath = sys.argv[1]
    inFiles = sys.argv[2]

    if os.path.exists(inPath):
        print 'In path exists : ' + inPath
    else:
        print 'Missing cmd line path : ' + inPath
        sys.exit(1)
        
    print 'Cmd line arguments look good'
else:
   print 'cmd line arguments missing: $ python  FCCFixCRFiles.py  inPath files'
   print
   print 'python FCCFixCRFiles.py C:\FCC\ CO.dat,CP.dat,LF.dat,SF.dat'
   sys.exit(0)


for oneFile in inFiles.split(','):
    FixFCCFile(inPath, oneFile)

# Clean up
for files in os.listdir(inPath):
    if files.endswith('.out'):
        os.remove(inPath + files)
        
print
print 'Done with FCCFixBadChars.py'


       
