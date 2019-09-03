import pymongo
client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.get_database('tg')

DB_SELFINFO = 'self_info'
DB_MESSAGE = 'message'
DB_ADMIN_LOG = 'admin_log'
DB_USER = 'user'
DB_CHANNEL = 'channel'
DB_SUPERGROUP = 'supergroup'
DB_CHATPARTICIPANTS = 'chat_participants'
DB_MEDIA = 'media'
DB_FORWARD = 'forward'
DB_RESUME = 'resume'
DB_RESUMEENTITY = 'resume_entity'
DB_RESUMEMEDIA = 'resume_media'