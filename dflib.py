import os
import tempfile
import subprocess
import itertools
import shutil

TEMP_DIR = '/env/dataflow/tmp'


class MapperFilter(object):
    def __init__(self, trans, name=None, printopt=False):
        self.trans = trans
        self.name = name
        self.printopt = printopt

    def run(self, read_fname, write_fname):
        with open(read_fname) as in_fobj, open(write_fname, 'w') as out_fobj:
            for line in in_fobj:
                output = self.trans(line)
                if output:
                    out_fobj.write(str(output) + '\n')                    

class MSepFilter(object):
    def __init__(self, trans, name=None, insep="\t", outsep="\t", printopt=False):
        self.trans = trans
        self.name = name
        self.printopt = printopt

    def run(self, read_fname, write_fname):
        with open(read_fname) as in_fobj, open(write_fname, 'w') as out_fobj:
            for line in in_fobj:
                linelist = line.strip().split(insep) #set insep to None to do split()
                outputlist = self.trans(linelist)
                if outputlist:
                    output_str = str(outsep.join(outputlist))
                    out_fobj.write(str(output) + '\n')                    
        

class ReducerFilter(object):
    def __init__(self, trans, keyfunc=lambda x: x.strip().split('\t')[0], 
                       printkey=True, name=None, printopt=False):
        self.trans = trans
        self.keyfunc = keyfunc
        self.name = name
        self.printopt = printopt
        self.printkey = printkey
    
    def run(self, read_fname, write_fname):
        with open(read_fname) as in_fobj, open(write_fname, 'w') as out_fobj:
            for key, line_iter in itertools.groupby(in_fobj, self.keyfunc):
                output = self.trans(line_iter)
                if output:
                    if self.printkey:
                        out_fobj.write(key + '\t' + str(output) + '\n')
                    else:
                        out_fobj.write(str(output) + '\n')                    

class ShellFilter(object):
    def __init__(self, cmd, name=None, printopt=False):
        self.cmd = cmd
        self.name = name
        self.printopt = printopt
    
    def run(self, read_fname, write_fname):
        total_cmd = 'export LC_COLLATE=C && cat ' + read_fname + ' | ' + self.cmd + ' > ' + write_fname
        subprocess.check_call(total_cmd, shell=True)



class DataChain(object):
    def __init__(self, dfilters, 
                 working_dir=tempfile.mkdtemp(dir=TEMP_DIR),
                 printopt=False):
        self.dfilters = dfilters
        self.printopt = printopt
        self.working_dir = working_dir
        print working_dir

    def run(self, input_fname, output_fname):
        read_fname = input_fname        
        for ii, dfilter in enumerate(self.dfilters):
            appendage = dfilter.name if dfilter.name else "dchain_" + str(ii)
            input_basename = os.path.basename(input_fname)
            write_basename = input_basename + "." + appendage
            write_fname = os.path.join(self.working_dir, write_basename)
            print write_fname
            dfilter.run(read_fname, write_fname)
            read_fname = write_fname

            # BUG SOMEWHERE BELOW. DELETING TOO EARLY?
            ## we clean up, based on printopt settings
            #if ii==0: #never delete the input file
            #    pass
            #elif self.printopt == False: #DataChain overrides individual Filter printopts
            #    os.remove(read_fname)
            #elif self.printopt == "Native": #DataChain defers to dfilter printopts
            #    if dfilter.printopt == False: #dfilter says to delete
            #        os.remove(read_fname)
            #elif self.printopt == True:
            #    pass
            #else:
            #    raise ValueError("DataChain printopt set to something other than True, False or \"Native\"")
        
        shutil.copyfile(write_fname, output_fname)    

    def run_test(self, input_fname, output_fname, nlines=1000):
        input_basename = os.path.basename(input_fname)
        smaller_input_fname = os.path.join(self.working_dir, input_basename + '_test')
        head_cmd = 'head ' + input_fname + ' -n ' + str(nlines) + ' > ' + smaller_input_fname
        subprocess.check_call(head_cmd, shell=True)
        self.run(smaller_input_fname, output_fname)

        
if __name__=="__main__":

    def mtrans(line):
        eltlist = line.strip().split("\t")
        outlist = eltlist[:2]
        out_str = "\t".join(outlist)
        return out_str
    
    def isum(line_iter):
        agg = 0
        for line in line_iter:
            agg += float(line.strip().split('\t')[1])
        return str(agg)

    mfilter = MapperFilter(trans=mtrans)
    usort = ShellFilter(cmd="sort")
    rfilter = ReducerFilter(trans=isum)
    
    in_file = "/workplace/dataflow/src/dataflow/data/rdata.txt"
    mid_file = "/workplace/dataflow/src/dataflow/data/rdata.txt.mout"
    mid_2_file = "/workplace/dataflow/src/dataflow/data/rdata.txt.m2out"
    out_file = "/workplace/dataflow/src/dataflow/data/rdata.txt.out"
    
    mfilter.run(in_file, mid_file)
    usort.run(mid_file, mid_2_file)
    rfilter.run(mid_2_file, out_file)
   
    mychain = DataChain([mfilter, usort, rfilter])
    mychain.run(in_file, "/workplace/dataflow/src/dataflow/data/rdata.txt.out.chain")
    mychain.run_test(in_file, "/workplace/dataflow/src/dataflow/data/rdata.txt.out.chain_1000", nlines=1000)
