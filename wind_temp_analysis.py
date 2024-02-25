import re
from mrjob.job import MRJob

QUALITY_RE = re.compile(r"[01459]")

class WindTempAnalysis(MRJob):

    def mapper(self, _, line):
        val = line.strip()
        wind_direction = val[60:63]
        temp = val[87:92]
        quality = val[92:93]
        wind_quality = val[63:64]

        if wind_direction != '999' and temp != "+9999" and re.match(QUALITY_RE, quality) and re.match(QUALITY_RE, wind_quality):
            yield wind_direction, int(temp)

    def reducer(self, key, values):
        temperatures = list(values)
        min_temp = min(temperatures)
        max_temp = max(temperatures)
        count = len(temperatures)
        yield key, {"low": min_temp, "high": max_temp, "count": count}

if __name__ == '__main__':
    WindTempAnalysis.run()
