class NodeStatsDeltaMapper:
    latest_stats = {}
    prev_stats = {}
    
    def __init__(self, latest_stats, prev_stats):  
        self.prev_stats = prev_stats  
        self.latest_stats = latest_stats  
    
    def getAvgQueryTime(self):
        queryTime = self.getQueryTime()
        queryTotal = self.getQueryTotal()
        return (queryTime/queryTotal) if queryTotal > 0 else 0

    def getAvgQueryThenFetchTime(self):
        fetchTime = self.getFetchTime()
        fetchTotal = self.getFetchTotal()
        fetchAvg = (fetchTime/fetchTotal) if fetchTotal > 0 else 0
        return fetchAvg + self.getAvgQueryTime()

    def getQueryTime(self):
        return self.getDeltaVal("indices.search.query_time_in_millis")

    def getQueryTotal(self):
        return self.getDeltaVal("indices.search.query_total")

    def getFetchTime(self):
        return self.getDeltaVal("indices.search.fetch_time_in_millis")

    def getFetchTotal(self):
        return self.getDeltaVal("indices.search.fetch_total")

    def getMergeTotal(self):
        return self.getDeltaVal("indices.merges.total_size_in_bytes")

    def getWriteOperations(self):
        return self.getDeltaVal("fs.io_stats.total.write_operations")
    
    def getReadOperations(self):
        return self.getDeltaVal("fs.io_stats.total.read_operations")

    def getDeltaVal(self, key):
        prevVal = dict_get(self.prev_stats, key)
        latestVal = dict_get(self.latest_stats, key)
        return latestVal - prevVal

def dict_get(dict_obj: dict, keys: str, default = False):
        keys = keys.split('.')
        for key in keys:
            if key in dict_obj:
                dict_obj = dict_obj[key]
            else:
                return default
        return dict_obj      

