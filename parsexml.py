import re
from tokenizer import tokenize, TOK
import csv


class Header:
    def __init__(self):
        self.id = -1
        self.title = ''
        self.ns = 0
        self.text = []


def strip_tag_name(t):
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


def parse_xml():
    total_count = 0
    revision = 0
    filename = 'enwiki-latest-pages-articles-multistream1.xml-p1p30303'
    # filename = 'test.xml'

    i = 0
    countpages = 0
    page = False
    readtext = False

    with open(filename) as file:
        with open('data.csv', 'w', newline='') as dataFile:
            writer = csv.writer(dataFile)
            writer.writerow(["Id", "Title", "NS", "TEXT"])
            for line in file:
                if line.strip() == "<page>" and not page:
                    header = Header()
                    id = False
                    page = True
                    countpages += 1
                    buffer = []

                if line.strip().startswith("<title>"):
                    header.title = line.strip().replace('<title>', '').replace('</title>', '')
                    continue

                if line.strip().startswith("<ns>"):
                    header.ns = line.strip().replace('<ns>', '').replace('</ns>', '')
                    continue

                if line.strip().startswith("<id>") and not id:
                    header.id = int(line.strip().replace('<id>', '').replace('</id>', ''))
                    id = True
                    continue

                if line.strip().startswith('<redirect title="'):
                    header.redirect = line.strip().replace('<redirect title="', '').replace('" />', '')
                    continue

                if line.strip().startswith('<text'):
                    buffer = re.sub("<[^>]*>", "", line.strip())
                    if line.replace('\n', '').endswith('</text>') or buffer.upper().startswith("#REDIRECT"):
                        readtext = False
                    else:
                        readtext = True
                    continue
                if readtext:
                    if line.strip().endswith("</text>"):
                        buffer = buffer + line.strip().replace("</text>", '')
                        header.text.append(buffer)
                        buffer = ''
                        readtext = False
                        writer.writerow([header.id, header.title, header.ns, header.text])

                        print("I: " + str(countpages) + " Title: " + header.title + " NS: " + header.ns + " ID: " + str(
                            header.id))

                    else:
                        if line == '\n':
                            header.text.append(buffer)
                            buffer = ''
                        else:
                            buffer = buffer + line.strip()

                if line.strip() == "</page>" and page:
                    page = False
                i += 1

    print(countpages)


if __name__ == "__main__":
    parse_xml()
