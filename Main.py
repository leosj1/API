from flask import Flask, request, jsonify
from flask_restful import Resource, Api
import Terms.terms as t
import json
import Cluster.clusters as c
import Terms.API_TTERMS as apitest
import collections
import time


app = Flask(__name__)

api = Api(app)


class Test(Resource):
    def get(self):
        return {'Testing': 'Clustering'}

    def post(self):
        some_json = request.get_json()
        tracker_id = some_json['tracker_id']
        type_ = some_json['type']
        conf = t.getconf2()

        def compare(x, y): return collections.Counter(
            x) == collections.Counter(y)

        try:
            if type_ == 'create':
                # time.sleep(3)
                if len(tracker_id) > 0:
                    t.insert_terms(tracker_id, str(
                        {}), 'blogsite_id in ()', str({}), conf)
                    print('tid--', tracker_id, len(tracker_id))
                    tracker_details = t.query(
                        conf, f'select query from trackers where tid = {tracker_id}')
                    prev_tids = tracker_details[0][0].replace(
                        'blogsite_id in (', '').replace(')', '').strip()
                    prev_terms_ids = t.query(conf, f'select query from tracker_keyword where tid = {tracker_id}')[
                        0][0].replace('blogsite_id in (', '').replace(')', '').strip()

                    prev_tid_split = t.cleanbrackets(prev_tids).split(',')
                    prev_terms_ids_split = t.cleanbrackets(
                        prev_terms_ids).split(',')

                    if not compare(prev_tid_split, prev_terms_ids_split):

                        print('create', tracker_id)
                        if __name__ == '__main__':
                            apitest.testingKWT(tracker_id, '144.167.35.89')
                            return jsonify({'success': 'success-created'})

            elif type_ == 'update':
                print('in update', tracker_id)
                result = ''
                tid = ''
                new_ids = ''
                if '******' in tracker_id:
                    tid = tracker_id.split('******')[0]
                    new_ids = tracker_id.split('******')[1].split(',')
                elif '------' in tracker_id:
                    tid = tracker_id.split('------')[0]
                    new_ids = tracker_id.split('------')[1].split(',')

                tracker_details = t.query(
                    conf, f'select query from trackers where tid = {tid}')
                prev_tids = tracker_details[0][0].replace(
                    'blogsite_id in (', '').replace(')', '').strip()
                print('prev_tids', prev_tids)

                prev_terms_ids = t.query(conf, f'select query from tracker_keyword where tid = {tid}')[
                    0][0].replace('blogsite_id in (', '').replace(')', '').strip()

                print('prev_terms_ids', prev_terms_ids)
                if '******' in tracker_id:
                    id_found = []
                    id_not_found = []

                    for id_ in new_ids:
                        if id_ in prev_tids.split(','):
                            id_found.append(id_)
                        else:
                            id_not_found.append(id_)

                    prev_tid_split = t.cleanbrackets(prev_tids).split(',')
                    prev_terms_ids_split = t.cleanbrackets(
                        prev_terms_ids).split(',')

                    if not compare(prev_tid_split, prev_terms_ids_split):
                        try:
                            apitest.testingKWT(tid, '144.167.35.89')
                            return jsonify({'success': 'success-updated'})

                        except Exception as e:
                            print('exception at kwt', e)

                        return result

                elif '------' in tracker_id:
                    print(tid)

                    prev_tid_split = t.cleanbrackets(prev_tids).split(',')
                    prev_terms_ids_split = t.cleanbrackets(
                        prev_terms_ids).split(',')

                    if not compare(prev_tid_split, prev_terms_ids_split):
                        apitest.testingKWT(tid, '144.167.35.89')
                        return jsonify({'success': f'{new_ids} blog(s) deleted'})

                return jsonify({'success': 'No changes made'})
            elif type_ == 'delete':
                t.delete_terms_and_cluster(tracker_id, conf)
                return jsonify({'success': 'Tracker and details deleted'})
            else:
                return jsonify({'error': result})
        except Exception as e:
            result = str(e)
            return jsonify({'error': result})


class Multi(Resource):
    def get(self, num):
        return {'result': num * 2}

    def post(self, num):

        return {'result': num * 2}


class Clusters(Resource):
    def post(self):
        conf = c.getconf2()
        some_json = request.get_json()

        tid = some_json['tracker_id']

        tracker_id = tid

        tracker_details = t.query(conf, f'select query from trackers where tid = {tracker_id}')
        print('------------------------------------------------------')
        print('tid---------',tracker_id)
        print('tid---------',tracker_details)
        print('------------------------------------------------------')

        count_q = t.query(conf, f'select count(*) from blogposts where {tracker_details[0][0]}')

        count_cluster = t.query(
            conf, f'select total from clusters where tid = {tracker_id}')
        if not count_cluster:
            count_cluster = [[0]]
            c.insert_single_cluster(tid, 0, 0, conf)
            print('-------here')

        print('tracker_details--', count_cluster[0][0])

        if count_q[0][0] != count_cluster[0][0]:
            try:
                all_ = c.getClusterforall(tid, conf)
                c.insert_to_cluster(conf, all_[0], tid)
                c.getStatus(conf, tid, 100)
                print('tid-----', tid)
            except Exception as e:
                print(e)

            return tid


api.add_resource(Test, '/')
api.add_resource(Clusters, '/clusterings')
api.add_resource(Multi, '/Multi/<int:num>')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, threaded=True, port=5001)
