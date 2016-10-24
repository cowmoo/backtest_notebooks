import zipfile
import datetime

def get_historical_option_iterator(f, instrument):
    last_line = None
    for line in f:        
        if line.startswith(instrument + ","):
            last_line = line 
            break
            
    while True and last_line.startswith(instrument + ","):
        yield last_line
        last_line = f.readline()

def parse_csv_entry(line):

    def get_date(date_str):
        return datetime.datetime.strptime(date_str, '%m/%d/%Y') #01/19/2018

    columns = line.split(",")
    underlying_price = columns[1]
    opt_symbol = columns[3]
    opt_type = columns[5]
    expiration = columns[6]
    data_date = columns[7]
    strike = columns[8]
    last = columns[9]
    bid = columns[10]
    ask = columns[11]
    #volume = columns[12]
    volume = 2000 # hard-code the volume to make the options tradeable
    
    return {"Symbol": opt_symbol, "DateType": get_date(data_date), "Type": opt_type, "ExpDate": get_date(expiration), "Strike": strike, "Last": last, "Bid": bid, "Ask": ask, "Volume": volume, "UnderlyingPrice": underlying_price}

def parse_historical_option_data(zip_file, file_handle, instrument):
    entries = {}
    
    with zipfile.ZipFile(zip_file, 'r') as myzip:
        for day_file in myzip.namelist():
            print "FileName:" + day_file
            f = myzip.open(day_file)
            line = f.readline()
            it = get_historical_option_iterator(f, instrument)
        
            for entry in it:
		parsed_entry = parse_csv_entry(entry)
		current_date = parsed_entry['DateType']
		expiration_date = parsed_entry['ExpDate']
		strike_price = float(parsed_entry['Strike'])
		underlying_price = float(parsed_entry['UnderlyingPrice'])
		
		if (expiration_date - current_date) <= datetime.timedelta(days=90) and strike_price > underlying_price * 0.875 and strike_price < underlying_price * 1.125:
                	file_handle.write(entry)

def filter_historical_options(zip_array, instrument, outfile):
    target = open(outfile, 'w')
    
    for zip_file in zip_array:
        parse_historical_option_data(zip_file, target, instrument)
        
    target.close()

zip_array = ['bb_2014_January.zip', 'bb_2014_February.zip', 'bb_2014_March.zip', 'bb_2014_April.zip', 'bb_2014_May.zip', 'bb_2014_June.zip', 'bb_2014_July.zip', 'bb_2014_August.zip', 'bb_2014_September.zip','bb_2014_October.zip', 'bb_2014_November.zip', 'bb_2014_December.zip', 'bb_2015_January.zip', 'bb_2015_February.zip', 'bb_2015_March.zip', 'bb_2015_April.zip', 'bb_2015_May.zip', 'bb_2015_June.zip', 'bb_2015_July.zip', 'bb_2015_August.zip', 'bb_2015_September.zip','bb_2015_October.zip', 'bb_2015_November.zip', 'bb_2015_December.zip', 'bb_2016_January.zip', 'bb_2016_February.zip', 'bb_2016_March.zip', 'bb_2016_April.zip', 'bb_2016_May.zip', 'bb_2016_June.zip', 'bb_2016_July.zip', 'bb_2016_August.zip', 'bb_2016_September.zip']
filter_historical_options(zip_array, "SPX", "3yr_SPX_filtered.csv")
