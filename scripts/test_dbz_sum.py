import psycopg2
import os, glob
from collections import namedtuple
#todo sunday
def dbz_to_mm_h(dbz):
    if (dbz != "nan"):
        #print("input for dbz check")
        #print(dbz)
        math_dbz = float(10**(float(dbz)/10))
        math_dbz = float(math_dbz/200)
        math_dbz = pow(math_dbz,(0.625))
        #print(math_dbz)
        #input("wait")
        return math_dbz

a = 21.2922
b = 7.34706
sum_mm_h = 0.78094 + 0.06827
 
sum_dbz = a + b

print("suma dbz")
print(dbz_to_mm_h(sum_dbz))
print("suma mm_h")
print(sum_mm_h)