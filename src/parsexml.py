import re
import csv
from src.model.Header import Header


def strip_tag_name(t):
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


def parse_xml():
    total_count = 0
    revision = 0
    filename = '../data/in/All/enwiki-latest-pages-articles-multistream1.xml-p1p30303'
    # filename = '../data/in/Sample/test.xml'

    i = 0
    countpages = 0
    page = False
    read_text = False

    with open(filename) as file:

        header_list = ["Id", "Title", "NS", "Model", "Format"]

        with open('../data/out/csv/data.csv', 'w', newline='') as dataFile:
            writer = csv.writer(dataFile)
            writer.writerow(header_list)

            for line in file:

                if line.strip() == "<page>" and not page:
                    header = Header()
                    id = False
                    page = True
                    countpages += 1
                    buffer = []

                if line.strip().startswith("<title>"):
                    header.set_title(line.strip().replace('<title>', '').replace('</title>', ''))
                    continue

                if line.strip().startswith("<ns>"):
                    header.set_ns(line.strip().replace('<ns>', '').replace('</ns>', ''))
                    continue

                if line.strip().startswith("<id>") and not id:
                    header.set_id(int(line.strip().replace('<id>', '').replace('</id>', '')))
                    id = True
                    continue

                if line.strip().startswith('<model>'):
                    header.set_model(line.strip().replace('<model>', '').replace('</model>', ''))
                    continue

                if line.strip().startswith('<format>'):
                    header.set_format(line.strip().replace('<format>', '').replace('</format>', ''))
                    continue

                if line.strip().startswith('<text'):
                    buffer = re.sub("<[^>]*>", "", line.strip())
                    if line.replace('\n', '').endswith('</text>') or buffer.upper().startswith("#REDIRECT"):
                        read_text = False
                    else:
                        read_text = True
                    continue

                if read_text:
                    if line.strip().endswith("</text>"):

                        buffer = buffer + line.strip().replace("</text>", '')
                        header.append_text(buffer)
                        header.parse_text()

                        buffer = ''
                        read_text = False

                        writer.writerow(header.get_list())

                        if countpages % 1000 == 0:
                            print(countpages)

                    else:
                        if line == '\n':
                            header.append_text(buffer)
                            buffer = ''
                        else:
                            buffer = buffer + line.strip()

                if line.strip() == "</page>" and page:
                    page = False
                i += 1

    print(countpages)


if __name__ == "__main__":
    parse_xml()
