from flask import Flask, request, jsonify
from flask_restful import Resource, Api
import terms as t
import keywordtrend as kwt
import json
import pooltest2 as pt
import clusters as c
import  API_TTERMS as apitest
import collections


app = Flask(__name__)

api = Api(app)


class Test(Resource):
    def get(self):
        return {'Testing' : 'Clustering'}

    def post(self):
        some_json = request.get_json()
        tracker_id = some_json['tracker_id']
        type_ = some_json['type']
        conf = t.getconf()
        
        
        compare = lambda x, y: collections.Counter(x) == collections.Counter(y)
      
        try:
            if type_ == 'create':
                print('tid--',tracker_id,len(tracker_id))
                if len(tracker_id) > 0:
                    # result = t.getTerms(tracker_id,conf)
                    blod_ids = t.query(conf, f'select query from trackers where tid = {tracker_id}')[0][0]
                    # blod_ids = blod_ids.replace(')', '').replace('(', '')
                    t.insert_terms(tracker_id, str({}), 'blogsite_id in ()' , str({}),conf)

                    tracker_details = t.query(conf,f'select query from trackers where tid = {tracker_id}')
                    prev_tids = tracker_details[0][0].replace('blogsite_id in (', '').replace(')','').strip()
                    prev_terms_ids = t.query(conf,f'select query from tracker_keyword where tid = {tracker_id}')[0][0].replace('blogsite_id in (', '').replace(')','').strip()

                    prev_tid_split = t.cleanbrackets(prev_tids).split(',')
                    prev_terms_ids_split = t.cleanbrackets(prev_terms_ids).split(',')

                    
                    if not compare(prev_tid_split, prev_terms_ids_split):
                        
                        # terms_Result = result_[0][0]
                        print('create',tracker_id)
                        
                        # t.update_terms(tracker_id, result, conf)
                        if __name__ == '__main__':
                            apitest.testingKWT(tracker_id)  
                        # result_tuple = pt.testingKWT(tracker_id)                        
                        # result = result_tuple[0]
                        # result_total = result_tuple[1]

                        # blod_ids = t.query(conf, f'select query from trackers where tid = {tracker_id}')[0][0]
                        # # t.insert_terms(tracker_id, t.sort_dict(result_total,50), f'{blod_ids}' , result,conf)
                        # t.update_terms(tracker_id, t.sort_dict(result_total,50), blod_ids,result , conf)
                        # print(tracker_id, result_total, f'blogsite_id in {blod_ids}' ,conf)
                            return jsonify({'success':'success-created'})
                        # return result_total
                # else:

                
            elif type_ =='update':
                print('in update',tracker_id)
                result = ''
                tid = ''
                new_ids = ''
                if '******' in tracker_id:
                    tid = tracker_id.split('******')[0]
                    new_ids = tracker_id.split('******')[1].split(',')
                elif '------' in tracker_id:
                    tid = tracker_id.split('------')[0]
                    new_ids = tracker_id.split('------')[1].split(',')

                tracker_details = t.query(conf,f'select query from trackers where tid = {tid}')
                prev_tids = tracker_details[0][0].replace('blogsite_id in (', '').replace(')','').strip()
                print('prev_tids',prev_tids)
                
                prev_terms_ids = t.query(conf,f'select query from tracker_keyword where tid = {tid}')[0][0].replace('blogsite_id in (', '').replace(')','').strip()

                final_terms = {}
                print('prev_terms_ids', prev_terms_ids)
                if '******' in tracker_id:
                    id_found = []
                    id_not_found = []
                    new_update = []
                    not_found_add = ','
                    for id_ in new_ids:
                        if id_ in prev_tids.split(','):
                            id_found.append(id_)
                        else:
                            id_not_found.append(id_)

                    prev_tid_split = t.cleanbrackets(prev_tids).split(',')
                    prev_terms_ids_split = t.cleanbrackets(prev_terms_ids).split(',')

                    
                    if not compare(prev_tid_split, prev_terms_ids_split):
                        print('updating')
                        # id_joined = not_found_add.join(id_found)
                        # new_terms = dict(t.getandUpdateTerms(id_joined, conf))
                        # prev_terms = t.query(conf,f'select terms from tracker_keyword where tid = {tid}')[0][0]                        

                        # result = prev_terms

                        # if len(prev_terms) > 2 and prev_terms != '{}':
                        #     pass
                    
                        # result_tuple = pt.testingKWT(tid)
                        
                        # result = result_tuple[0]
                        # # print('result',result)
                        # result_total = result_tuple[1]
                        # print('result_total',result_total)

                        try:
                            # pass
                            # t.update_terms(tid, t.sort_dict(result_total,50),  f'blogsite_id in ({prev_tids})',result , conf)
                            apitest.testingKWT(tid)  
                            return jsonify({'success':'success-updated'})

                            # kwt.trendforall(tid, conf)

                        except Exception as e:
                            print('exception at kwt', e)

                        # return jsonify({'success':'success-updated'})
                        return result

                elif '------' in tracker_id: 
                    print(tid)
                    found = []
                    not_found = []
                    not_found_add = ''
                    # current_blog_ids = t.cleanbrackets(prev_tids)
                    # result = dict(t.getandUpdateTerms(current_blog_ids, conf))
                    prev_tid_split = t.cleanbrackets(prev_tids).split(',')
                    prev_terms_ids_split = t.cleanbrackets(prev_terms_ids).split(',')
                
                    # t.update_terms(tid, result,  f'blogsite_id in ({current_blog_ids})', conf)
                    if not compare(prev_tid_split, prev_terms_ids_split):
                        apitest.testingKWT(tid)  
                    # return new_terms_update
                        return jsonify({'success':f'{new_ids} blog(s) deleted'})
                  
                return jsonify({'success':'No changes made'})
            elif type_ == 'delete':
                # result = t.getTerms(tracker_id,conf)
                t.delete_terms(tracker_id, conf)
                return jsonify({'success':'Tracker and details deleted'})
            else:
                return jsonify({'error':result})
        except Exception as e:
            result = str(e)
            return jsonify({'error':result})


class Multi(Resource):
    def get(self, num):
        return {'result' : num * 2}

    def post(self, num):
        some_json = request.get_json()
        return {'result' : num * 2}

class Clusters(Resource):
    def post(self):
        conf = c.getconf()
        some_json = request.get_json()
        
        tid = some_json['tracker_id']

        compare = lambda x, y: collections.Counter(x) == collections.Counter(y)

        tracker_id = tid

        tracker_details = t.query(conf,f'select query from trackers where tid = {tracker_id}')

        count_q = t.query(conf,f'select count(*) from blogposts where {tracker_details[0][0]}')

    
        count_cluster = t.query(conf,f'select total from clusters where tid = {tracker_id}')
        if not count_cluster:
            count_cluster=[[0]]
            print('-------here')

        print('tracker_details--',count_cluster[0][0])
        # print('somejson', some_json,prev_tid_split,prev_terms_ids_split)
        

        if count_q[0][0] != count_cluster[0][0]:
            try:            
                all_ = c.getClusterforall(tid, conf)
                c.insert_to_cluster(conf,all_[0], tid)
                c.getStatus(conf, tid, 100)
                print('tid-----',tid)
            except Exception as e:
                print(e)

            return tid


api.add_resource(Test, '/')
api.add_resource(Clusters, '/clusterings')
api.add_resource(Multi, '/Multi/<int:num>')

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True, threaded= True)