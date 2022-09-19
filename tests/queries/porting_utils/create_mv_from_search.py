import os, sys, argparse



def read_search(file_path, filter_patterns):
    print(f"read_search: file_path = {file_path}, filter_patterns = {filter_patterns}")
    matched_lines = []
    try:
        fd = open(file_path, 'r')
        lines = fd.readlines()
        fd.close()
        for line in lines:
            
            match_flag = 1
            for i in range(len(filter_patterns)):
                filter_pattern = filter_patterns[i]
                if i ==0 and filter_pattern in line:
                    match_flag = 1  
                else:
                    match_flag = 0
                print(f"line = {line}, match_flag = {match_flag}")
                
                line_items = line.split("\n")
                line_items = line_items[0].split(".")
                
                last_line_item = line_items[-1]
                last_line_item_str = '.'+last_line_item #when line_items = line_items[0].split("."), "." is gone, so add it back for matching
                print(f"line_items = {line_items}, last_line_item_str = {last_line_item_str}, filter_patterns = {filter_patterns}")
                if last_line_item_str in filter_patterns:
                    match_flag = 1
                else:
                    match_flag = 0
                
                print(f"line = {line}, match_flag = {match_flag}")

                match_flag = match_flag * match_flag
        
            if match_flag == 1:
                matched_lines.append(line)
        matched_lines = list(set(matched_lines))
        for line in matched_lines:
            print(line)
    except (BaseException) as error:
        print(f"_read_sql_from_files exception, error: {error}")    
    return matched_lines


def find_and_replace(lines, search_patterns, replace_patterns):
    if len(search_patterns) != len(replace_patterns):
        raise Exception(f"number of search_patterns = {len(search_patterns)} not equals replace_pattern number = {len(replace_patterns)}")
    processed_lines = []
    for line in lines:
        for i in range(len(search_patterns)):
            search_pattern = search_patterns[i]
            replace_pattern = replace_patterns[i]
            if search_pattern in line:
                line = line.replace(search_pattern, replace_pattern)
        processed_lines.append(line)
    for line in processed_lines:
        print(line)
    
    return processed_lines

def lines_2_sh(sh_file_path, lines_2_write):
    try:
        fd = open(sh_file_path, 'w')
        lines = fd.writelines(lines_2_write)
        fd.close()

    except (BaseException) as error:
        print(f"process_lines_2_sh exception, error == {error}")
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("vs_search_result_file", help="the file the script to work on to replace items match search_patterns with replace_patterns")
    parser.add_argument("sh_file", help="sh file to be created based on the processing of the vs_search_result_file")
    parser.add_argument("-f", "--filter_patterns", required=True, help="patterns for filter lines from search results", default="")
    parser.add_argument("-s", "--search_patterns", required=True, help="search patterns to be matched for replace in search results")
    parser.add_argument("-r", "--replace_patterns", required=True, help="patterns to replace items in lines matched search patterns")
    args = parser.parse_args()
    print(f"args = {args}")
    search_file = args.vs_search_result_file
    sh_file = args.sh_file
    filter_patterns = args.filter_patterns.split(',')
    search_patterns = args.search_patterns.split(',')
    replace_patterns = args.replace_patterns.split(',')
    print(f"search_file = {search_file}, filter_patterns = {filter_patterns}, search_patterns = {search_patterns}, replace_patterns = {replace_patterns}")

    mached_lines = read_search(search_file, filter_patterns)
    processed_lines = find_and_replace(mached_lines, search_patterns, replace_patterns)
    
    lines_2_sh(sh_file, processed_lines)


