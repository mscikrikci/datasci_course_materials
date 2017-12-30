import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    if record[0]=='a':
        for i in range(5):
            mr.emit_intermediate((record[1],i),[record[2],record[3]])
    else:
        for i in range(5):
            mr.emit_intermediate((i,record[2]),[record[1],record[3]])

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    for x in range(5):
        values = filter(lambda i: i[0] == x, list_of_values)
        if len(values) <= 1:
            i_val = 0
        else:
            i_val = reduce(lambda x, y: x[1] * y[1], values)
        total = total + i_val
    mr.emit((key[0], key[1], total))
    
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
