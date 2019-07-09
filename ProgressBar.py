import pyodbc
import datetime
import sys
#Alternative is to use:
#pip install progressbar
## Usage:
##>> python fixdb.py 9

WO=sys.argv[1]

PM_DB = r"C:\Users\e162186\Desktop\FixCCTVDB\PM_WO_Main.mdb"
PM_WO_Table = "PM_WO" + str(WO)

PM_allLines = r"C:\Users\e162186\Desktop\FixCCTVDB\AllPMLines.mdb"
PM_allLines_Table = "PM_AllLines"

Inspection_DB_File = "Contract_WO" + str(WO) + ".mdb"

Inspection_DB = r"C:\Users\e162186\Desktop\FixCCTVDB" + "\\" + Inspection_DB_File

conAccess_PMDB = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + PM_DB + ";")
cursor_PMDB = conAccess_PMDB.cursor()


conAccess_PMDB_All = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + PM_allLines + ";")
cursor_PMDB_All = conAccess_PMDB_All.cursor()


def correctManholeName(manholeName):
    manholeName = str(manholeName)
    manholeName = manholeName.upper()
    manholeName = manholeName.replace(' ','')
    manholeName = manholeName.replace('-','')
    return manholeName

def getFacilityID_PMWO(upstream_MH, downstream_MH):
    global PM_DB
    global PM_WO_Table
    global PM_allLines
    global PM_allLines_Table
    
    global conAccess_PMDB
    global cursor_PMDB
    
    
    upstream_MH = correctManholeName(upstream_MH)
    downstream_MH= correctManholeName(downstream_MH)

    
    cursor_PMDB.execute("SELECT * FROM {0}".format(PM_WO_Table))
    results = cursor_PMDB.fetchall()
    try:
        for row in results:
            if((upstream_MH ==str(row[3]) and downstream_MH == str(row[4])) or (upstream_MH ==str(row[4]) and downstream_MH == str(row[3]))):
                #print("Found FacilityID: " + str(row[1]))
                return str(row[1])
    except Exception as e:
        print("Error: ")
        print(e)
    return "NO"
    print(">> Not Found")

def makeArrow(nbr):
    ind_prog = "="*(nbr-1)
    ind_prog = ind_prog+"=>"
    return ind_prog

def getFacilityID_AllLines(upstream_MH, downstream_MH):
    global PM_allLines
    global PM_allLines_Table
    
    global conAccess_PMDB_All
    global cursor_PMDB_All
    
    
    upstream_MH = correctManholeName(upstream_MH)
    downstream_MH= correctManholeName(downstream_MH)
    

    cursor_PMDB_All.execute("SELECT * FROM {0}".format(PM_allLines_Table))

    results = cursor_PMDB_All.fetchall()
    return_val = "NO"
    try:
        for row in results:
            if((upstream_MH == str(row[3]) and downstream_MH == str(row[4])) or (upstream_MH == str(row[4]) and downstream_MH == str(row[3]))):
                #print("Found FacilityID: " + str(row[1]))
                return_val= str(row[1])
    except Exception as e:
        print("Error: ")
        return_val = "NO"
        print(e)
    
    if(return_val == "NO"):
        return_val = getFacilityID_PMWO(upstream_MH,downstream_MH)
    return return_val

conAccess_InspectionDB = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + Inspection_DB + ";")
cursor_InspectionDB = conAccess_InspectionDB.cursor()



cursor_InspectionDB.execute("SELECT * FROM Inspections")

count = 1
results = cursor_InspectionDB.fetchall()
total_record = len(results)
count_found = 0
count_notFound = 0
not_found_list = []
logFile = PM_WO_Table +"_"+ datetime.datetime.now().strftime("%I_%M%p_%B_%d_%Y") + ".log.csv"

with open(logFile, 'w') as outfile:
    outfile.write('InspectionID|Pipe_Segment_Reference|Upstream_MH|Downstream_MH|WorkOrder\n')
try:
    for row in results:
        count = count + 1
        percentage = count/total_record*100
        if(percentage >= 100):
            percentage = 100
        str_out = "[" + str(round(percentage, 2)) + "%] Inspection ID: {4} >> Pipe_Segment_Reference: {0} | Upstream_MH: {1} | Downstream_MH: {2} | WorkOrder: {3}".format(row[7],row[13],row[17],row[46],row[0])
        str_out_write = "{4}|{0}|{1}|{2}|{3}".format(row[7],row[13],row[17],row[46],row[0])
        #print(str_out)
        sys.stdout.write('\r')
        #print("[" + str(round(percentage, 2)) + "%]")
        #sys.stdout.write("[" + str(round(percentage, 2)) + "%]")
        sys.stdout.write("[%-1s] %d%%" % (makeArrow(int(percentage)), percentage))
        sys.stdout.flush()
        facilityID = getFacilityID_AllLines(str(row[13]),str(row[17]))
        if( facilityID == "NO"):
            count_notFound = count_notFound + 1
            not_found_list.append(str_out)
            with open(logFile, 'a') as outfile:
                outfile.write(str(str_out_write) + "\n")
        else:
            count_found = count_found + 1
            update_facilityID_cmd = "UPDATE Inspections SET Pipe_Segment_Reference = '{0}' WHERE InspectionID = {1}".format(facilityID,int(row[0]));
            cursor_InspectionDB.execute(update_facilityID_cmd)
except Exception as e:
    print("Error: ")
    print(e)

print("\n========================================================================================================")
print("                                        Below lines are not found")
print("========================================================================================================")
for elm in not_found_list:
    print(elm)

print("========================================================================================================")
print("Inspection Database: WorkOrder " + str(WO))
print("Total rows: ",count)
print("Found: ",count_found)
print("Not Found: ", count_notFound)
print("Not found percentage: ", str(round(count_notFound/total_record*100, 2)), "%")
print("========================================================================================================")

cursor_InspectionDB.commit()

conAccess_InspectionDB.close()
conAccess_PMDB.close()
conAccess_PMDB_All.close()