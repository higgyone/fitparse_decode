"""
Decode fitfile to get heart rate data and its timestamp
"""

import csv
import fitparse
import pytz
import datetime
from pathlib import Path

#choose the right directory hwere fit files are located
dir = "C:/temp/data/temp"
testdir = "C:/temp/data/test"
datadir = dir

UTC = pytz.UTC
GMT = pytz.timezone('Europe/London')

# heart rate data list
hrdata = []
# corresponding timestamp
timestamp = []

# globals to get the time data
current_timestamp = datetime.datetime.now()
timestamp_16 = None

def print_record(data):
    """
    test method to print out the data in a record
    """
    for record in data:
        for field in record:
            if field.units:
                print (" * %s: %s %s" % (
                    field.name, field.value, field.units))
            else:
                print (" * %s: %s" % (field.name, field.value))

def get_time(record):
    """
    extract the time from data. Note for heart rate data the timestamp is int(16) up to 65536 ~ 18hours
    This will overflow and needs normalised so it wont give a crazy result on overflow.

    Every now ans then there will be a timestamp giving the right date and time. 
    Heart rate data is timestamped on a rolling seconds counter. Need to set the first timestamp_16 to the
    global time then work out the next time based on the difference between timestamp_16s adding that to global time
    """
    new_timestamp_16 = None
    
    # difference between successive timestamp_16
    delta_diff = 0
    global timestamp_16
    global current_timestamp

    for r in record:
        if r.field is not None:
            if r.name == 'timestamp_16':
                new_timestamp_16 = r.value

    # if the timestamp is none then set it to the global time
    if new_timestamp_16 is not None:
        if timestamp_16 is None:
            timestamp_16 = new_timestamp_16
        
        delta_diff = new_timestamp_16 - timestamp_16
        
        # deal with overflow
        if delta_diff < 0:
            delta_diff += 65536
        
        current_timestamp += datetime.timedelta(seconds=delta_diff)
        timestamp_16 = new_timestamp_16
        #print(current_timestamp)
        return current_timestamp

def get_heartrate(record):
    """
    Return the heart rate from the record
    """
    for r in record:
        if r.name == 'heart_rate':
            return r.value

def output_messages(fitfile):
    """
    Go through the fitfile and extract time and heart rate data
    """
    messages = fitfile.get_messages()

    global current_timestamp
    global timestamp_16

    global hrdata
    global timestamp

    for record in messages:
    # Go through all the data entries in this record

        # Extract the global time stamp
        if record.name == 'monitoring_info':
            for r in record.fields:
                if r.field is not None:
                    if r.field.name == 'local_timestamp':
                        current_timestamp = r.value
                        timestamp_16 = None
                        #print("Timestamp: {}".format(current_timestamp))

        # get the heart rate data
        if record.name == 'monitoring':
            for r in record:
                if r.field is not None:
                    if r.field.name =='heart_rate':
                        time = get_time(record)
                        hr = get_heartrate(record)

                        timestamp.append(time)
                        hrdata.append(hr)
                        #print("Time: {}   Heart: {}".format(time,hr))
 

def output_csv(time, hr, fi):
    """
    Write the extracted time and heart rate data to csv fiile
    """
    data = [time, hr]

    csvFile = open(fi,'w')
    with csvFile:
        print("Writing csv to: {}".format(fi))
        writer = csv.writer(csvFile, lineterminator='\n') # need lineterminator to prevent extra linebreaks
        writer.writerow(["Date time", "Heart rate BPM"])
        for i in range(len(time)):
            writer.writerow([time[i],hr[i]])


def main():

    # get the fit files in order (maybe)
    files = sorted(Path(datadir).glob('*.fit'))

    for f in files:

        ff = datadir+'/'+f.name

        fitfile = fitparse.FitFile(ff)

        print("converting {}".format(f.name))

        output_messages(fitfile)
   
    fi = datadir+'/'+'op.csv'
    output_csv(timestamp, hrdata, fi)

if __name__=='__main__':
    main()