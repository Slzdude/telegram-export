from sqlalchemy import Column, ForeignKey, Integer, Table, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class ChatParticipant(Base):
    __tablename__ = 'ChatParticipants'

    ContextID = Column(Integer, primary_key=True, nullable=False)
    DateUpdated = Column(Integer, primary_key=True, nullable=False)
    Added = Column(Text, nullable=False)
    Removed = Column(Text, nullable=False)


class Forward(Base):
    __tablename__ = 'Forward'

    ID = Column(Integer, primary_key=True)
    OriginalDate = Column(Integer, nullable=False)
    FromID = Column(Integer)
    ChannelPost = Column(Integer)
    PostAuthor = Column(Text)


class Media(Base):
    __tablename__ = 'Media'

    ID = Column(Integer, primary_key=True)
    Name = Column(Text)
    MimeType = Column(Text)
    Size = Column(Integer)
    ThumbnailID = Column(ForeignKey('Media.ID'))
    Type = Column(Text)
    LocalID = Column(Integer)
    VolumeID = Column(Integer)
    Secret = Column(Integer)
    Extra = Column(Text)

    parent = relationship('Media', remote_side=[ID])


class Resume(Base):
    __tablename__ = 'Resume'

    ID = Column(Integer, nullable=False)
    ContextID = Column(Integer, primary_key=True)
    Date = Column(Integer, nullable=False)
    StopAt = Column(Integer, nullable=False)


class ResumeEntity(Base):
    __tablename__ = 'ResumeEntity'

    ID = Column(Integer, primary_key=True, nullable=False)
    ContextID = Column(Integer, primary_key=True, nullable=False)
    AccessHash = Column(Integer)


class ResumeMedia(Base):
    __tablename__ = 'ResumeMedia'

    MediaID = Column(Integer, primary_key=True)
    ContextID = Column(Integer, nullable=False)
    SenderID = Column(Integer)
    Date = Column(Integer)


class SelfInformation(Base):
    __tablename__ = 'SelfInformation'

    ID = Column(Integer, primary_key=True)
    UserID = Column(Integer)


class Version(Base):
    __tablename__ = 'Version'

    ID = Column(Integer, primary_key=True)
    Version = Column(Integer)


class AdminLog(Base):
    __tablename__ = 'AdminLog'

    ID = Column(Integer, primary_key=True, nullable=False)
    ContextID = Column(Integer, primary_key=True, nullable=False)
    Date = Column(Integer, nullable=False)
    UserID = Column(Integer)
    MediaID1 = Column(ForeignKey('Media.ID'))
    MediaID2 = Column(ForeignKey('Media.ID'))
    Action = Column(Text)
    Data = Column(Text)

    Media = relationship('Media', primaryjoin='AdminLog.MediaID1 == Media.ID')
    Media1 = relationship('Media', primaryjoin='AdminLog.MediaID2 == Media.ID')


class Channel(Base):
    __tablename__ = 'Channel'

    ID = Column(Integer, primary_key=True, nullable=False)
    DateUpdated = Column(Integer, primary_key=True, nullable=False)
    About = Column(Text)
    Title = Column(Text, nullable=False)
    Username = Column(Text)
    PictureID = Column(ForeignKey('Media.ID'))
    PinMessageID = Column(Integer)

    Media = relationship('Media')


class Chat(Base):
    __tablename__ = 'Chat'

    ID = Column(Integer, primary_key=True, nullable=False)
    DateUpdated = Column(Integer, primary_key=True, nullable=False)
    Title = Column(Text, nullable=False)
    MigratedToID = Column(Integer)
    PictureID = Column(ForeignKey('Media.ID'))

    Media = relationship('Media')


class Message(Base):
    __tablename__ = 'Message'

    ID = Column(Integer, primary_key=True, nullable=False)
    ContextID = Column(Integer, primary_key=True, nullable=False)
    Date = Column(Integer, nullable=False)
    FromID = Column(Integer)
    Message = Column(Text)
    ReplyMessageID = Column(Integer)
    ForwardID = Column(ForeignKey('Forward.ID'))
    PostAuthor = Column(Text)
    ViewCount = Column(Integer)
    MediaID = Column(ForeignKey('Media.ID'))
    Formatting = Column(Text)
    ServiceAction = Column(Text)

    Forward = relationship('Forward')
    Media = relationship('Media')


class Supergroup(Base):
    __tablename__ = 'Supergroup'

    ID = Column(Integer, primary_key=True, nullable=False)
    DateUpdated = Column(Integer, primary_key=True, nullable=False)
    About = Column(Text)
    Title = Column(Text, nullable=False)
    Username = Column(Text)
    PictureID = Column(ForeignKey('Media.ID'))
    PinMessageID = Column(Integer)

    Media = relationship('Media')


class User(Base):
    __tablename__ = 'User'

    ID = Column(Integer, primary_key=True, nullable=False)
    DateUpdated = Column(Integer, primary_key=True, nullable=False)
    FirstName = Column(Text, nullable=False)
    LastName = Column(Text)
    Username = Column(Text)
    Phone = Column(Text)
    Bio = Column(Text)
    Bot = Column(Integer)
    CommonChatsCount = Column(Integer, nullable=False)
    PictureID = Column(ForeignKey('Media.ID'))

    Media = relationship('Media')
