import time

import pymongo

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.get_database('tg')

db_self_info = db['self_info']
db_dialog = db['dialog']
db_message = db['message']
db_admin_log = db['admin_log']
db_user = db['user']
db_channel = db['channel']
db_super_group = db['supergroup']
db_chat = db['chat']
db_chat_participants = db['chat_participants']
db_media = db['media']
db_forward = db['forward']
db_resume = db['resume']
db_resume_entity = db['resume_entity']
db_resume_media = db['resume_media']

db_message.create_index('context_id')
db_resume.create_index('context_id')
db_resume_entity.create_index('context_id')
db_resume_media.create_index('context_id')
db_chat_participants.create_index('context_id')


def update_by_id(col, row):
    return col.update_one({'id': row['id']}, {'$set': row}, upsert=True)


def update_by_invalidation_time(col, row, t):
    ret = col.find_one({'id': row['id']})
    if not ret or time.time() - ret['date_updated'] > t:
        ret = col.update_one({'id': row['id']}, {'$set': row}, upsert=True)
    return ret
