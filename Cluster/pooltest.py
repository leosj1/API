import pandas as pd
import concurrent.futures
import functions
from tqdm import tqdm 
from pathos.multiprocessing import ProcessPool

util = functions.util
insert = functions.insert

def main():
    blogposts = pd.read_json(r'C:\Users\dev\Desktop\TOPTERMS\BLOGPOSTS.json')

    lastcol = ''
    with open('C:\\spark\\test\\lastcolumn.txt') as fi:
        lastcol = fi.readlines()[0]
        fi.close()

    blogposts = blogposts[blogposts["blogpost_id"] > int(lastcol)].to_dict('records')

    def process_blogs(records):
        import functions
        blogpost_id = str(records['blogpost_id'])
        blogsite_id = str(records['blogsite_id'])
        post = str(records['post'])
        date = str(records['date'])

        values = blogpost_id,blogsite_id, post, date
        
        val = functions.util(values)
        print('finished cleaning')
        functions.insert(val)
        print(f'{blogpost_id} has been processed and inserted')

    parallel = True


    if parallel: 
        pool = ProcessPool(5)
        # for _ in pool.uimap(process_blogs, blogposts):
        #     pass
        for _ in tqdm(pool.uimap(process_blogs, blogposts), ascii=True, total=len(blogposts)):
            pass
    else:
        for line in tqdm(blogposts):
            process_blogs(line)


if __name__ == "__main__":
    main()