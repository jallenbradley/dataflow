import flow

input_file  = ${INPUT}
output_file = ${OUTPUT}
working_dir = ${WORKING_DIR}

mapper_func = ${MAPPER_LAMBDA}
${MAPPER_DEF}def mapper_func(line):
${MAPPER_DEF}    out_str = 
${MAPPER_DEF}    return out_str

reducer_func = ${REDUCER_LAMBDA}
${REDUCER_DEF}def reducer_func(line_iter):
${REDUCER_DEF}    out_str = 
${REDUCER_DEF}    return out_str

${REDUCER_KEY_GETTER_DEF}reducer_key_getter = lamda x: x.strip().split("\t")[0]
reducer_key_getter = ${REDUCER_KEY_GETTER_LAMBDA}

mapper_filter  = flow.MapperFilter(trans=mapper_func, name="map")
usort_filter   = flow.ShellFilter(cmd="sort")
reducer_filter = flow.ReducerFilter(trans=reducer_func, name="red")

dflist = [ 
           mapper_filter,
           usort_filter,
           reducer_filter
         ]

flow.DataChain(dflist, working_dir=working_dir).run(input_file, output_file)


