#! /workplace/dataflow/df_python/bin/python
import argparse
import re
import os

# templates
map_template_fname = '/workplace/dataflow/src/dataflow/templates/map_template.py'

# general replacements
input_regex = regex = re.compile('.*(\$\{INPUT\}).*')
output_regex = regex = re.compile('.*(\$\{OUTPUT\}).*')
working_dir_regex = regex = re.compile('.*(\$\{WORKING_DIR\}).*')

# map specific replacements
mapper_lambda_regex = regex = re.compile('.*(\$\{MAPPER_LAMBDA\}).*')
mapper_def_regex = regex = re.compile('.*(\$\{MAPPER_DEF\}).*')


def process_general_template(args, template_fname):
    abs_input = os.path.abspath(args.input)
    abs_output = os.path.abspath(args.output)
    abs_working_dir = os.path.abspath(args.working_dir)
    partially_replaced_text = []
    with open(template_fname) as template_fobj:
        for line in template_fobj:
            if input_regex.match(line):
                line = line.replace("${INPUT}", "\"" + abs_input + "\"")
            if output_regex.match(line):
                line = line.replace("${OUTPUT}", "\"" + abs_output + "\"")
            if working_dir_regex.match(line):
                if args.working_dir:
                    line = line.replace("${WORKING_DIR}", "\"" + abs_working_dir + "\"")
                else:
                    line = line.replace("${WORKING_DIR}", "None")
            partially_replaced_text.append(line)        
    return partially_replaced_text

def map_chainmaker(args):
    with open(args.chain_file, 'w') as chain_file:
        partially_replaced_text = process_general_template(args, map_template_fname)
        for line in partially_replaced_text:
            if mapper_lambda_regex.match(line):
                if args.mapper:
                    line = line.replace("${MAPPER_LAMBDA}", args.mapper)
                else:
                    continue #deleting this line if mapper is not passed
            if mapper_def_regex.match(line):
                if args.mapper:
                    line = line.replace("${MAPPER_DEF}", "#")
                else:
                    line = line.replace("${MAPPER_DEF}", "")
            chain_file.write(line)
            

if __name__=="__main__":
   parser = argparse.ArgumentParser()
   parser.add_argument("input", type=str, help="input file name")
   parser.add_argument("output", type=str, help="output file name")
   parser.add_argument("chain_file", type=str, help="code file name")
   parser.add_argument("-wd", "--working_dir", type=str, help="working directory for datachain")
   parser.add_argument("-t", "--test", action="store_true", help="run chain in test mode")
   #parser.add_argument("--run", action="store_true", help="run the chain")

   subparsers = parser.add_subparsers(help='different chain structures')
   
   map_parser = subparsers.add_parser('map', help='single mapper structure')
   map_parser.add_argument("--mapper", default='', help='python function to use as mapper') 
   map_parser.set_defaults(func=map_chainmaker)

   args = parser.parse_args()
   args.func(args)



   

