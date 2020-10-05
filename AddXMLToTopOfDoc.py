import fileinput

filename = 'enwiki-latest-pages-articles-multistream1.xml-p1p30303'
line_to_prepend = '<?xml version="1.0" encoding="UTF-8"?>\n'

f = fileinput.input(filename, inplace=1)
for xline in f:
    if f.isfirstline():
        print(line_to_prepend + xline)
    else:
        print(xline.rstrip('\n'))
