###############################################################################
## VisTrails package for reading and extracting data from files that
## python's csv module can parse.
##
## By David Koop, dkoop@poly.edu
##
## Copyright (C) 2012, NYU-Poly.
## Copyright (C) 2010-2011, University of Utah.
###############################################################################

import copy
import csv
import operator

from core.modules.vistrails_module import Module, ModuleError

class TabularData(Module):
    def __init__(self, data, header=None):
        self.data = data
        self.header = header

class CSVReader(Module):
    _input_ports = [('file', '(edu.utah.sci.vistrails.basic:File)')]
    _output_ports = [('data', '(edu.utah.sci.dakoop.tabdata:TabularData)')]
    
    def compute(self):
        reader = csv.reader(open(self.getInputFromPort('file').name, 'rU'))
        reader.next()
        reader.next()
        header = reader.next()
        data = []
        for row in reader:
            data.append(row)
        tab_data = TabularData(data, header)
        self.setResult('data', tab_data)

class RankByColumn(Module):
    _input_ports = [('data', '(edu.utah.sci.dakoop.tabdata:TabularData)'),
                    ('columnName', '(edu.utah.sci.vistrails.basic:String)'),
                    ('column', '(edu.utah.sci.vistrails.basic:Integer)')]
    _output_ports = [('data', '(edu.utah.sci.vistrails.basic:List)')]
    
    def compute(self):
        d = self.getInputFromPort('data')
        data = d.data
        header = d.header
        
        if self.hasInputFromPort('columnName'):
            if header is None:
                raise ModuleError(self, "Data does not contain header")
            column_name = self.getInputFromPort('columnName')
            try:
                idx = header.index(column)
            except ValueError:
                raise ModuleError(self, "Data does not contain column '%s'" % \
                                      column)
        else:
            idx = self.getInputFromPort('column')

        data = copy.deepcopy(data)
        data.sort(key=lambda x: x[idx])
        d = TabularData(data, header)
        
class ExtractColumn(Module):
    _input_ports = [('data', '(edu.utah.sci.dakoop.tabdata:TabularData)'),
                    ('columnName', '(edu.utah.sci.vistrails.basic:String)'),
                    ('column', '(edu.utah.sci.vistrails.basic:Integer)')]
    _output_ports = [('columnData', '(edu.utah.sci.vistrails.basic:List)')]
    
    def compute(self):
        d = self.getInputFromPort('data')
        data = d.data
        header = d.header
        
        if self.hasInputFromPort('columnName'):
            if header is None:
                raise ModuleError(self, "Data does not contain header")
            column_name = self.getInputFromPort('columnName')
            try:
                idx = header.index(column_name)
            except ValueError:
                raise ModuleError(self, "Data does not contain column '%s'" % \
                                      column)
        else:
            idx = self.getInputFromPort('column')
            
        col_data = []
        for row in data:
            print 'processing row', row
            col_data.append(row[idx])
        self.setResult('columnData', col_data)

class ExtractRow(Module):
    _input_ports = [('data', '(edu.utah.sci.dakoop.tabdata:TabularData)'),
                    ('rowName', '(edu.utah.sci.vistrails.basic:String)'),
                    ('row', '(edu.utah.sci.vistrails.basic:Integer)')]
    _output_ports = [('rowData', '(edu.utah.sci.vistrails.basic:List)')]
    
    def compute(self):
        d = self.getInputFromPort('data')
        data = d.data
        header = d.header
        
        if self.hasInputFromPort('rowName'):
            row_name = self.getInputFromPort('rowName')
            found = False
            for idx, row in enumerate(rows):
                if row[0] == row_name:
                    found = True
                    break
            if not found:
                raise ModuleError(self, "Data does not contain row '%s'" % \
                                      row)
        else:
            idx = self.getInputFromPort('row')

        row_data = data[idx]
        self.setResult('rowData', row_data)

class JoinData(Module):
    _input_ports = [('dataA', '(edu.utah.sci.dakoop.tabdata:TabularData)'),
                    ('dataB', '(edu.utah.sci.dakoop.tabdata:TabularData)'),
                    ('joinColA', '(edu.utah.sci.vistrails.basic:Integer)'),
                    ('joinColB', '(edu.utah.sci.vistrails.basic:Integer)')]
    _output_ports = [('data', '(edu.utah.sci.dakoop.tabdata:TabularData)')]

    def compute(self):
        a_d = self.getInputFromPort('dataA')
        b_d = self.getInputFromPort('dataB')
        a_data = a_d.data
        b_data = b_d.data

        a_join = self.getInputFromPort('joinColA')
        b_join = self.getInputFromPort('joinColB')
        
        b_idx = {}
        for idx, row in enumerate(b_data):
            b_idx[row[b_join]] = idx
        
        data = []
        for row in a_data:
            a_idx = row[a_join]
            new_row = row + b_data[b_idx[a_idx]]
            data.append(new_row)

        header = a_d.header + b_d.header
        joined_data = TabularData(data, header)
        self.setResult('data', joined_data)

class AggregateData(Module):
    _input_ports = [('data', '(edu.utah.sci.dakoop.tabdata:TabularData)'),
                    ('columns', '(edu.utah.sci.vistrails.basic:List)'),
                    ('operation', '(edu.utah.sci.vistrails.basic:String)')]
    _output_ports = [('data', '(edu.utah.sci.dakoop.tabdata:TabularData)')]

    def compute(self):
        d = self.getInputFromPort('data')
        data = d.data
        header = d.header
        columns = self.getInputFromPort('columns')

        operation = self.getInputFromPort('operation')
        if not hasattr(operator, operation):
            raise ModuleError("Unknown operation: '%s'" % operation)
        op = getattr(operator, operation)

        new_data = []
        for row in data:
            new_row = []
            value = None
            for idx in xrange(len(row)):
                raw_val = row[idx]
                new_row.append(raw_val)
                if idx in columns:
                    # col_val = int(''.join(raw_val.split(',')))
                    if value is None:
                        value = raw_val
                    else:
                        value = op(value, raw_val)
            new_row.append(value)
            new_data.append(new_row)
            
        new_header = None
        if header is not None:
            new_header = []
            for idx, col in enumerate(header):
                if idx not in columns:
                    new_header.append(col)
            new_header.append("Aggregate")

        new_d = TabularData(new_data, new_header)
        self.setResult('data', new_d)

class StringToNumeric(Module):
    _input_ports = [('data', '(edu.utah.sci.dakoop.tabdata:TabularData)')]
    _output_ports = [('data', '(edu.utah.sci.dakoop.tabdata:TabularData)')]
    
    def compute(self):
        d = self.getInputFromPort('data')
        data = d.data

        new_data = []
        for row in data:
            new_row = []
            for item in row:
                new_item = item
                try:
                    new_item = float(''.join(item.split(',')))
                    new_item = int(''.join(item.split(',')))
                except ValueError:
                    pass
                new_row.append(new_item)
            new_data.append(new_row)
        
        new_d = TabularData(new_data, d.header)
        self.setResult('data', new_d)

_modules = [TabularData, CSVReader, ExtractColumn, ExtractRow, JoinData,
            AggregateData, StringToNumeric]
