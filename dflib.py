import tempfile

TEMP_DIR = '/tmp'


class MapperFilter(object):
    def __init__(trans, name, root_dir=TEMP_DIR, printopt=False):
        self.root_dir = root_dir
        self.trans = trans
        self.name = name

    def run(in_stream, out_stream):
        for line in in_stream:
            output = self.trans(line)
            out_stream.write(output + '\n')                    


class DataChain(object):
    def __init__(df_list, 
                 working_dir=tempfile.mkdtemp(prefix=TEMP_DIR)
                 printopt=False):
        self.df_list = df_list
        self.printopt = printopt

    def run(chain_input, chain_output):
        in_stream = open(chain_input)
        for ii, df in enumerate(self.df_list):
            out_fname = os.path.join(working_dir, df.name)
            out_stream = open(out_fname, 'w')
            
            df.run(in_stream, out_stream)
            
            out_stream.close()
            
            in_stream = open(out_fname)
        
        in_stream.close()
        shutil.copyfile(out_fname, chain_output)    

        
        
        

