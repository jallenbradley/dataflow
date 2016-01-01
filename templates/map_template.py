import flow

input_file  = ${INPUT}
output_file = ${OUTPUT}
working_dir = ${WORKING_DIR}

mapper_func = ${MAPPER_LAMBDA}
${MAPPER_DEF}def mapper_func(line):
${MAPPER_DEF}    out_str = 
${MAPPER_DEF}    return out_str

mfilter = flow.MapperFilter(trans=mapper_func, name="mapper")

dflist = [ 
           mfilter
         ]

flow.DataChain(dflist, working_dir=working_dir).run(input_file, output_file)


