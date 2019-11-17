import csv


class FileWriter(object):

    def __init__(self, file_output):
        self.file_output = file_output


class CsvFileWriter(FileWriter):

    def __init__(self, iterator, file_output):
        super().__init__(file_output)
        self.iterator = iterator

    def process(self):
        ite_result = self.iterator.read()

        with open(self.file_output, 'w') as csv_file:
            # csv_file =  open(path_file_out, 'w')

            npi_ele = next(ite_result )
            w = csv.DictWriter(csv_file, npi_ele.keys())
            w.writeheader()

            for npi_ele in ite_result:
                w.writerow(npi_ele)

        # with open(path_file_out, 'w') as csv_file:
        #     wr = csv.writer(csv_file, dialect='excel')
        #     wr.writerows(members)