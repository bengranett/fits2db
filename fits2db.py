import sys,os
import pyfits

user = ''
dbname = ''
password = ''

batchsize=10000

formats = {'D':'DOUBLE',
           'E':'FLOAT',
           'L': 'TINYINT',
           'X': 'BIT',
           'B': 'TINYINT',
           'I': 'INT',
           'J': 'INT',
           'K': 'BIGINT',
           'A': 'CHAR',
           #'C': 'float', # not implemented in mysql
           #'M': 'float',
           #'P': ,
           }

path = sys.argv[1]
outpath = path + ".mysql"

fits = pyfits.open(sys.argv[1])

data = fits[1].data
table_name = fits[1].header['EXTNAME']

table_name = table_name.replace("W1","")
table_name = table_name.replace("W4","")
if table_name.startswith("_"):
    table_name = table_name[1:]
print "table name",table_name

columns = data.columns

out = file(outpath,"w")

print >>out, "CREATE TABLE IF NOT EXISTS %s ("%table_name

col = []
i = 0
for v in columns:
    i+=1
    if i > 1:
        print >>out, ","

    n,c = v.format[:-1],v.format[-1]

    if c=='A':
        s = "%s(%s)"%(formats[c],n)
    else:
        s = formats[c]

    extra = ''
    if v.name=='num':
        extra = ' PRIMARY KEY'

    print >>out, "  %s %s%s"%(v.name, s, extra),
    col.append((v.name,c))
    print v.name,c

print >>out, ");"


start = "INSERT INTO %s VALUES"%table_name

print >>out, start

stuff = []
for name,fmt in col:
    stuff.append(data.field(name))

n,m = len(stuff[0]),len(stuff)

for i in range(n):
    a = []
    for j in range(m):
        name,fmt = col[j]
        if fmt=='A':
            s = "\"%s\""%stuff[j][i]
        else:
            s = "%s"%str(stuff[j][i])

        a.append(s)

    comma = ","
    if i%batchsize==batchsize-1:
        comma=";"

    if i==n-1:  # end of table
        comma=""

    print >>out, "  (%s)%s"%(",".join(a), comma)

    if comma==";":
        print i
        print>>out,start

print >>out, ";"
out.close()

print "loading table..."

os.system("mysql -u %s --password=%s %s < %s"%(user, password, dbname, outpath))

print "done"
