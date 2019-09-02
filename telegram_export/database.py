from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from telegram_export.models import Base

engine = create_engine('mysql+pymysql://root:@127.0.0.1:3306/tg?charset=utf8', echo=True)
Session = sessionmaker(bind=engine)

session = Session()

Base.metadata.create_all(engine)
