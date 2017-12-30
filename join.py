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
    key = record[1]
    value = record
    #words = value.split()
    #for w in words:
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    #total = 0
    #for v in list_of_values:
    #  total += v
    orders = filter(lambda record: record[0]=="order",list_of_values)
    items = filter(lambda record: record[0]=="line_item",list_of_values)
    for o in orders:
       for i in items:
          if o[1]==i[1]:
             mr.emit(o+i)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
