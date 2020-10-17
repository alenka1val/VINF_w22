import re
import csv
from dateutil.parser import parse

REGEX = "((?<=[^0-9a-z])( *)((([0-3]{1}[0-9]{1})([^0-9a-z]))|((jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)([^0-9])))([^0-9a-z])( *)((([0-3]{1}[0-9]{1})([^0-9a-z]))|((jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)([^0-9])))( *)(([0-9]{4})))"


class Header:

    def __init__(self):
        self.id = -1
        self.title = ''
        self.ns = 0
        self.text = []
        self.model = ''
        self.format = ''

    def set_id(self, id):
        self.id = id

    def set_title(self, title):
        self.title = title

    def set_ns(self, ns):
        self.ns = ns

    def set_model(self, model):
        self.model = model

    def set_format(self, format):
        self.format = format

    def append_text(self, buffer):
        self.text.append(buffer)

    def get_list(self):
        return [self.id, self.title, self.ns, self.model, self.format]

    def parse_text(self):

        header_list = ["Wiki_id", "Date", "Paragraph"]
        print("Header_id: {}".format(self.id))
        if self.id == 572:
            print("now")

        with open('../data/out/csv/date.csv', 'w', newline='') as dataFile:

            writer = csv.writer(dataFile)
            writer.writerow(header_list)

            for i, paragraph in enumerate(self.text):

                if i == 0:
                    self.append_text(re.findall("{{.*}}", paragraph)[0])

                dates = re.findall(REGEX, paragraph, re.IGNORECASE)

                for date in dates:
                    dt = parse(date[0])
                    writer.writerow([self.id, dt.date(), paragraph])
